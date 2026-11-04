package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/yukino-js/shita.go/yukino-cloud-storage/pkg/bundle"
)

// Config holds the storage service settings. It corresponds to the C++
// Storage.conf file but is loaded explicitly and validated, rather than through
// a racy lazily-initialised singleton defined in a header.
type Config struct {
	// ServerIP is the interface to bind. Empty or "0.0.0.0" binds all.
	ServerIP string `json:"server_ip"`
	// ServerPort is the TCP port to listen on.
	ServerPort int `json:"server_port"`
	// DownloadPrefix is the URL prefix that routes to the download handler.
	DownloadPrefix string `json:"download_prefix"`
	// DeepStorageDir holds compressed bundles.
	DeepStorageDir string `json:"deep_storage_dir"`
	// LowStorageDir holds files stored verbatim.
	LowStorageDir string `json:"low_storage_dir"`
	// StorageInfoFile is the JSON file that persists the metadata table.
	StorageInfoFile string `json:"storage_info"`
	// BundleFormat names the compression codec used for deep storage.
	BundleFormat string `json:"bundle_format"`
	// TempDir is where deep-storage downloads are materialised so range
	// requests can be served. Empty means the OS default temporary directory.
	TempDir string `json:"temp_dir"`
	// MaxUploadBytes bounds an upload body. Zero selects the default.
	MaxUploadBytes int64 `json:"max_upload_bytes"`
}

const (
	defaultDownloadPrefix = "/download/"
	defaultBundleFormat   = "gzip"
	defaultMaxUploadBytes = int64(2 << 30) // 2 GiB
)

// DefaultConfig returns a configuration populated with safe defaults.
func DefaultConfig() *Config {
	return &Config{
		ServerIP:        "0.0.0.0",
		ServerPort:      8081,
		DownloadPrefix:  defaultDownloadPrefix,
		DeepStorageDir:  "./deep_storage/",
		LowStorageDir:   "./low_storage/",
		StorageInfoFile: "./storage.data",
		BundleFormat:    defaultBundleFormat,
		MaxUploadBytes:  defaultMaxUploadBytes,
	}
}

// LoadConfig reads, normalises and validates a JSON configuration file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("storage: read config: %w", err)
	}
	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("storage: parse config %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate normalises empty fields and rejects invalid values.
func (c *Config) Validate() error {
	if c.ServerPort <= 0 || c.ServerPort > 65535 {
		return fmt.Errorf("storage: server_port %d out of range", c.ServerPort)
	}
	if c.DownloadPrefix == "" {
		c.DownloadPrefix = defaultDownloadPrefix
	}
	// Normalise the prefix so both URL construction (prefix + name) and route
	// registration (prefix + "/*name") behave; for example "download" and
	// "/download" both become "/download/". Without this a prefix lacking the
	// trailing slash would yield URLs such as "/downloadname".
	if !strings.HasPrefix(c.DownloadPrefix, "/") {
		c.DownloadPrefix = "/" + c.DownloadPrefix
	}
	if !strings.HasSuffix(c.DownloadPrefix, "/") {
		c.DownloadPrefix += "/"
	}
	if c.BundleFormat == "" {
		c.BundleFormat = defaultBundleFormat
	}
	if _, err := bundle.ParseFormat(c.BundleFormat); err != nil {
		return err
	}
	if c.MaxUploadBytes <= 0 {
		c.MaxUploadBytes = defaultMaxUploadBytes
	}
	if c.DeepStorageDir == "" {
		c.DeepStorageDir = "./deep_storage/"
	}
	if c.LowStorageDir == "" {
		c.LowStorageDir = "./low_storage/"
	}
	if c.StorageInfoFile == "" {
		c.StorageInfoFile = "./storage.data"
	}
	return nil
}

// Addr returns the listen address in host:port form.
func (c *Config) Addr() string {
	ip := c.ServerIP
	if ip == "" {
		ip = "0.0.0.0"
	}
	return fmt.Sprintf("%s:%d", ip, c.ServerPort)
}

// BundleCodec resolves the configured compression format, falling back to gzip.
func (c *Config) BundleCodec() bundle.Format {
	f, err := bundle.ParseFormat(c.BundleFormat)
	if err != nil {
		return bundle.FormatGzip
	}
	return f
}
