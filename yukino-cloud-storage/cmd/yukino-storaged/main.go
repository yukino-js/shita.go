// Command yukino-storaged runs the cloud storage server: an HTTP service for
// uploading, listing and downloading files across two storage tiers, with an
// asynchronous log system grafted on top.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/yukino-js/shita.go/yukino-cloud-storage/internal/logging"
	"github.com/yukino-js/shita.go/yukino-cloud-storage/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "yukino-storaged: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		storageCfg = flag.String("config", "configs/storage.json", "path to the storage service config")
		logCfg     = flag.String("log-config", "configs/log.json", "path to the async log config")
		logFile    = flag.String("log-file", "./logfile/storage", "base path for the rolling log file")
	)
	flag.Parse()

	cfg, err := storage.LoadConfig(*storageCfg)
	if err != nil {
		return err
	}

	logger, cleanup, err := logging.Setup(logging.Options{
		Name:          "storage",
		LogConfigPath: *logCfg,
		FilePath:      *logFile,
		FileMaxSize:   1 << 20,
		Stdout:        true,
		Source:        "yukino-storaged",
	})
	if err != nil {
		return err
	}
	defer cleanup()

	data, err := storage.NewManager(cfg.StorageInfoFile)
	if err != nil {
		logger.Errorf("load storage metadata: %v", err)
		return err
	}

	svc, err := storage.NewService(cfg, data, logger)
	if err != nil {
		logger.Errorf("build service: %v", err)
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Infof("starting yukino-storaged (deep=%s low=%s info=%s)",
		cfg.DeepStorageDir, cfg.LowStorageDir, cfg.StorageInfoFile)
	if err := svc.Run(ctx); err != nil {
		logger.Errorf("service stopped with error: %v", err)
		return err
	}
	logger.Infof("yukino-storaged stopped cleanly")
	return nil
}
