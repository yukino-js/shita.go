// Command yukino-logbackupd runs the remote log backup receiver. It accepts
// ERROR and FATAL records shipped by other processes over RPC and appends them
// to a file, so they survive a crash of the originating process.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/yukino-js/shita.go/yukino-cloud-storage/internal/logging"
	"github.com/yukino-js/shita.go/yukino-cloud-storage/pkg/logbackup"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "yukino-logbackupd: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		addr    = flag.String("addr", "0.0.0.0:8080", "address to listen on")
		out     = flag.String("out", "./logfile/backup.log", "file to append received records to")
		logCfg  = flag.String("log-config", "configs/log.json", "path to the async log config")
		logFile = flag.String("log-file", "./logfile/logbackupd", "base path for the rolling log file")
	)
	flag.Parse()

	logger, cleanup, err := logging.Setup(logging.Options{
		Name:          "logbackup",
		LogConfigPath: *logCfg,
		FilePath:      *logFile,
		FileMaxSize:   1 << 20,
		Stdout:        true,
		Source:        "yukino-logbackupd",
		// This process is the backup receiver; it must not back up to itself.
		DisableBackup: true,
	})
	if err != nil {
		return err
	}
	defer cleanup()

	srv := logbackup.NewServer(logbackup.ServerOptions{Path: *out})
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Errorf("listen on %s: %v", *addr, err)
		return err
	}
	logger.Infof("log backup receiver listening on %s, writing to %s", *addr, *out)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()

	select {
	case <-ctx.Done():
		logger.Infof("log backup receiver shutting down")
		srv.Stop()
		<-errCh
		logger.Infof("log backup receiver stopped cleanly")
		return nil
	case err := <-errCh:
		srv.Stop()
		if err != nil {
			logger.Errorf("receiver stopped with error: %v", err)
		}
		return err
	}
}
