package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Logger is the minimal logging surface the agent needs. The async logger
// satisfies it, but the agent stays decoupled from any concrete implementation.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// Duration decodes a JSON string such as "1s" or "500ms" into a duration. A
// bare integer is interpreted as milliseconds.
type Duration time.Duration

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		if s == "" {
			*d = 0
			return nil
		}
		dur, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		*d = Duration(dur)
		return nil
	}
	var ms int64
	if err := json.Unmarshal(b, &ms); err != nil {
		return err
	}
	*d = Duration(ms) * Duration(time.Millisecond)
	return nil
}

// Duration returns the value as a time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// Config holds the upload agent settings. The original hard-coded these as
// global constants; they are now configurable.
type Config struct {
	// ServerURL is the base URL of the storage service.
	ServerURL string `json:"server_url"`
	// LowDir is watched for files to store verbatim.
	LowDir string `json:"low_storage_dir"`
	// DeepDir is watched for files to store compressed.
	DeepDir string `json:"deep_storage_dir"`
	// StateFile records which files have already been uploaded.
	StateFile string `json:"state_file"`
	// ScanInterval is how often the directories are rescanned.
	ScanInterval Duration `json:"scan_interval"`
	// QuietPeriod is how long a file must be unmodified before it becomes
	// eligible, so files still being written are skipped.
	QuietPeriod Duration `json:"quiet_period"`
	// Timeout bounds a single upload request.
	Timeout Duration `json:"timeout"`
}

// DefaultConfig returns agent settings with safe defaults.
func DefaultConfig() *Config {
	return &Config{
		ServerURL:    "http://127.0.0.1:8081",
		LowDir:       "./low_storage/",
		DeepDir:      "./deep_storage/",
		StateFile:    "./storage.dat",
		ScanInterval: Duration(time.Second),
		QuietPeriod:  Duration(5 * time.Second),
		Timeout:      Duration(30 * time.Second),
	}
}

// LoadConfig reads and validates an agent configuration file. Durations are
// expressed as Go duration strings, for example "1s" or "500ms".
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("client: read config: %w", err)
	}
	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("client: parse config %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate normalises empty fields and rejects invalid values.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.ServerURL) == "" {
		return fmt.Errorf("client: server_url is required")
	}
	if c.ScanInterval <= 0 {
		c.ScanInterval = Duration(time.Second)
	}
	if c.QuietPeriod < 0 {
		c.QuietPeriod = 0
	}
	if c.Timeout <= 0 {
		c.Timeout = Duration(30 * time.Second)
	}
	if c.LowDir == "" {
		c.LowDir = "./low_storage/"
	}
	if c.DeepDir == "" {
		c.DeepDir = "./deep_storage/"
	}
	if c.StateFile == "" {
		c.StateFile = "./storage.dat"
	}
	return nil
}

// Agent watches local directories and uploads new or changed files.
type Agent struct {
	cfg   *Config
	state *Manager
	http  *http.Client
	log   Logger
}

// NewAgent builds an agent. The upload state is loaded eagerly so a corrupt
// state file fails fast.
func NewAgent(cfg *Config, log Logger) (*Agent, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	state, err := NewManager(cfg.StateFile)
	if err != nil {
		return nil, err
	}
	return &Agent{
		cfg:   cfg,
		state: state,
		http:  &http.Client{Timeout: cfg.Timeout.Duration()},
		log:   log,
	}, nil
}

// candidate is a file eligible for consideration during a scan.
type candidate struct {
	path       string
	tier       string
	identifier string
	modTime    time.Time
}

// Run scans and uploads until ctx is cancelled. Unlike the original, which
// looped forever calling a non-portable Sleep, this uses a ticker and honours
// cancellation for clean shutdown.
func (a *Agent) Run(ctx context.Context) error {
	ticker := time.NewTicker(a.cfg.ScanInterval.Duration())
	defer ticker.Stop()
	a.scanOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			a.log.Infof("upload agent stopping")
			return nil
		case <-ticker.C:
			a.scanOnce(ctx)
		}
	}
}

// ScanOnce performs a single scan-and-upload pass. It is useful for one-shot
// invocations and for tests.
func (a *Agent) ScanOnce(ctx context.Context) { a.scanOnce(ctx) }

func (a *Agent) scanOnce(ctx context.Context) {
	candidates, err := a.collect()
	if err != nil {
		a.log.Errorf("scan directories: %v", err)
		return
	}
	for _, c := range candidates {
		if ctx.Err() != nil {
			return
		}
		if !a.needsUpload(c) {
			continue
		}
		if err := a.upload(ctx, c.path, c.tier); err != nil {
			a.log.Errorf("upload %s: %v", c.path, err)
			continue
		}
		if err := a.state.Set(c.path, c.identifier); err != nil {
			a.log.Warnf("record state for %s: %v", c.path, err)
		}
		a.log.Infof("uploaded %s (%s storage)", c.path, c.tier)
	}
}

// collect lists every regular file in the watched directories.
func (a *Agent) collect() ([]candidate, error) {
	dirs := []struct{ path, tier string }{
		{a.cfg.LowDir, "low"},
		{a.cfg.DeepDir, "deep"},
	}
	var out []candidate
	for _, d := range dirs {
		files, err := scanFiles(d.path)
		if err != nil {
			return nil, err
		}
		for _, p := range files {
			st, err := os.Stat(p)
			if err != nil {
				a.log.Warnf("skip %s: %v", p, err)
				continue
			}
			out = append(out, candidate{
				path:       p,
				tier:       d.tier,
				identifier: identifier(st),
				modTime:    st.ModTime(),
			})
		}
	}
	return out, nil
}

// needsUpload reports whether a candidate should be uploaded: it must have been
// quiet for the configured period and differ from the last uploaded version.
func (a *Agent) needsUpload(c candidate) bool {
	if time.Since(c.modTime) < a.cfg.QuietPeriod.Duration() {
		return false
	}
	if old, ok := a.state.Get(c.path); ok && old == c.identifier {
		return false
	}
	return true
}

// upload streams a file to the server's /upload endpoint.
func (a *Agent) upload(ctx context.Context, path, tier string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	endpoint := strings.TrimSuffix(a.cfg.ServerURL, "/") + "/upload"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, f)
	if err != nil {
		return err
	}
	req.Header.Set("FileName", base64.StdEncoding.EncodeToString([]byte(filepath.Base(path))))
	req.Header.Set("StorageType", tier)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = st.Size()

	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Drain a bounded amount so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned %s", resp.Status)
	}
	return nil
}

// identifier builds the name-size-mtime token used to detect changes, matching
// the original agent's scheme.
func identifier(st os.FileInfo) string {
	return fmt.Sprintf("%s-%d-%d", st.Name(), st.Size(), st.ModTime().Unix())
}

// scanFiles lists the regular files directly inside dir, creating it if needed.
func scanFiles(dir string) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		out = append(out, filepath.Join(dir, e.Name()))
	}
	return out, nil
}
