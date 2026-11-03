// Package logging wires the asynchronous log system together with the optional
// remote backup sink for the command binaries. It keeps the per-binary setup in
// one place so each daemon configures logging the same way.
package logging

import (
	"fmt"
	"os"

	"github.com/yukino-js/shita.go/yukino-cloud-storage/pkg/asynclog"
	"github.com/yukino-js/shita.go/yukino-cloud-storage/pkg/logbackup"
)

// Options selects the sinks and backup behaviour for a logger.
type Options struct {
	// Name is the logger name.
	Name string
	// LogConfigPath is the async log JSON config. Empty means built-in defaults.
	LogConfigPath string
	// FilePath, when set, adds a rolling file sink with this base path.
	FilePath string
	// FileMaxSize is the roll size for the file sink. Zero selects 1 MiB.
	FileMaxSize int64
	// Stdout toggles the standard output sink.
	Stdout bool
	// Source identifies this process to the backup receiver.
	Source string
	// DisableBackup suppresses the remote backup sink even when the config
	// specifies one. The backup receiver sets this to avoid backing up to itself.
	DisableBackup bool
}

// Setup builds a logger from the options, optionally attaching a remote backup
// sink, registers it with the default manager, and returns it together with a
// cleanup function that drains and closes the logger.
func Setup(opts Options) (*asynclog.Logger, func(), error) {
	cfg := asynclog.DefaultConfig()
	if opts.LogConfigPath != "" {
		loaded, err := asynclog.LoadConfig(opts.LogConfigPath)
		if err != nil {
			return nil, nil, err
		}
		cfg = loaded
	}

	builder := asynclog.NewBuilder().
		Name(opts.Name).
		FromConfig(cfg).
		AsyncType(asynclog.AsyncSafe)

	if opts.Stdout {
		builder.Flusher(asynclog.NewStdoutFlusher(cfg.FlushModeValue()))
	}
	if opts.FilePath != "" {
		maxSize := opts.FileMaxSize
		if maxSize <= 0 {
			maxSize = 1 << 20
		}
		builder.Flusher(asynclog.NewRollFileFlusher(opts.FilePath, maxSize, cfg.FlushModeValue()))
	}

	var backup *logbackup.Client
	if !opts.DisableBackup && cfg.BackupAddr != "" && cfg.BackupPort > 0 {
		addr := fmt.Sprintf("%s:%d", cfg.BackupAddr, cfg.BackupPort)
		backup = logbackup.NewClient(logbackup.ClientOptions{
			Addr:   addr,
			Source: opts.Source,
			// Diagnostics go to stderr; logging them through this logger would
			// recurse into the backup path.
			Logf: func(format string, args ...any) {
				fmt.Fprintf(os.Stderr, format+"\n", args...)
			},
		})
		builder.Backup(backup)
	}

	logger, err := builder.Build()
	if err != nil {
		if backup != nil {
			_ = backup.Close()
		}
		return nil, nil, err
	}
	// Registering makes the logger reachable through asynclog.GetLogger. A name
	// collision is not fatal: the returned logger is still fully usable.
	_ = asynclog.Register(logger)

	cleanup := func() { _ = logger.Close() }
	return logger, cleanup, nil
}
