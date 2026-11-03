// Command clerk is a demo client for a yukino-kv cluster — the Go
// equivalent of the C++ caller example.
//
// Demo workload: put and read back the same key -count times:
//
//	clerk -f test.conf -count 500
//
// One-shot operations:
//
//	clerk -f test.conf -get x
//	clerk -f test.conf -put x=42
//	clerk -f test.conf -append x=!
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"

	"github.com/yukino-js/shita.go/yukino-kv/clerk"
	"github.com/yukino-js/shita.go/yukino-kv/config"
)

func main() {
	var (
		configFile = flag.String("f", "test.conf", "node address config file")
		count      = flag.Int("count", 500, "number of Put/Get rounds in demo mode")
		key        = flag.String("key", "x", "key used by the demo workload")
		getKey     = flag.String("get", "", "one-shot: fetch this key")
		putKV      = flag.String("put", "", "one-shot: store key=value")
		appendKV   = flag.String("append", "", "one-shot: append key=value")
		verbose    = flag.Bool("v", false, "enable debug logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	if err := run(*configFile, *count, *key, *getKey, *putKV, *appendKV, logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(configFile string, count int, key, getKey, putKV, appendKV string, logger *slog.Logger) error {
	nodes, err := config.Load(configFile)
	if err != nil {
		return err
	}
	addrs := make([]string, len(nodes))
	for i, n := range nodes {
		addrs[i] = n.Addr()
	}

	client, err := clerk.NewClerk(addrs, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	logger.Info("clerk connected", "clientId", client.ClientId(), "servers", addrs)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	switch {
	case getKey != "":
		value, err := client.Get(ctx, getKey)
		if errors.Is(err, clerk.ErrNoKey) {
			fmt.Printf("get %s: <no such key>\n", getKey)
			return nil
		}
		if err != nil {
			return err
		}
		fmt.Printf("get %s: %q\n", getKey, value)
	case putKV != "":
		k, v, ok := strings.Cut(putKV, "=")
		if !ok {
			return errors.New("-put expects key=value")
		}
		if err := client.Put(ctx, k, v); err != nil {
			return err
		}
		fmt.Printf("put %s=%q: ok\n", k, v)
	case appendKV != "":
		k, v, ok := strings.Cut(appendKV, "=")
		if !ok {
			return errors.New("-append expects key=value")
		}
		if err := client.Append(ctx, k, v); err != nil {
			return err
		}
		fmt.Printf("append %s+%q: ok\n", k, v)
	default:
		// Demo workload mirroring the C++ caller example.
		for i := count; i > 0; i-- {
			value := fmt.Sprint(i)
			if err := client.Put(ctx, key, value); err != nil {
				return fmt.Errorf("put %s=%s: %w", key, value, err)
			}
			got, err := client.Get(ctx, key)
			if err != nil && !errors.Is(err, clerk.ErrNoKey) {
				return fmt.Errorf("get %s: %w", key, err)
			}
			fmt.Printf("put %s=%s, get returns %q\n", key, value, got)
		}
	}
	return nil
}
