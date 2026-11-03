// Command yukino-storage-agent watches local directories and uploads new or
// changed files to the cloud storage server.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/yukino-js/shita.go/yukino-cloud-storage/internal/client"
	"github.com/yukino-js/shita.go/yukino-cloud-storage/internal/logging"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "yukino-storage-agent: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		cfgPath = flag.String("config", "configs/agent.json", "path to the agent config")
		logCfg  = flag.String("log-config", "configs/log.json", "path to the async log config")
		logFile = flag.String("log-file", "./logfile/agent", "base path for the rolling log file")
		once    = flag.Bool("once", false, "run a single scan and exit instead of looping")
	)
	flag.Parse()

	cfg, err := client.LoadConfig(*cfgPath)
	if err != nil {
		return err
	}

	logger, cleanup, err := logging.Setup(logging.Options{
		Name:          "agent",
		LogConfigPath: *logCfg,
		FilePath:      *logFile,
		FileMaxSize:   1 << 20,
		Stdout:        true,
		Source:        "yukino-storage-agent",
	})
	if err != nil {
		return err
	}
	defer cleanup()

	agent, err := client.NewAgent(cfg, logger)
	if err != nil {
		logger.Errorf("build agent: %v", err)
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Infof("starting yukino-storage-agent (server=%s low=%s deep=%s)",
		cfg.ServerURL, cfg.LowDir, cfg.DeepDir)
	if *once {
		agent.ScanOnce(ctx)
		return nil
	}
	return agent.Run(ctx)
}
