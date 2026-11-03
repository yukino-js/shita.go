// Command yukino-log writes a short burst of records through the async backend.
//
// One record is written to standard output before the backend is attached.
// The rest go to rolled files under -dir. Stop waits until those files have
// been flushed and synced.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	yukino_log "github.com/yukino-js/shita.go/yukino-log"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	dir := flag.String("dir", "logs", "directory that receives rolled log files")
	n := flag.Int("n", 1000, "number of info records to write")
	roll := flag.Int64("roll", 1<<20, "file size in bytes that forces a roll")
	flag.Parse()

	fmt.Println("yukino-log: this line is written to standard output")

	backend, err := yukino_log.NewAsync(yukino_log.AsyncOptions{
		Basename: filepath.Join(*dir, "yukino-log"),
		RollSize: *roll,
	})
	if err != nil {
		return err
	}
	if err := backend.Start(); err != nil {
		return err
	}

	logger := yukino_log.New(yukino_log.Options{})
	logger.Attach(backend)
	logger.Info("async backend attached")
	for i := 0; i < *n; i++ {
		logger.Infof("hello %d", i)
	}
	if err := logger.Flush(); err != nil {
		_ = backend.Stop()
		return err
	}
	if err := backend.Stop(); err != nil {
		return err
	}
	fmt.Printf("wrote %d records under %s\n", *n, *dir)
	return nil
}
