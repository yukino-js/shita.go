// Command kvserver runs one yukino-kv cluster node, or bootstraps a whole
// local cluster for experimentation.
//
// Single-node mode — serve the node with the given id from a config file:
//
//	kvserver -f test.conf -id 0
//
// Cluster bootstrap mode — the equivalent of the C++ raftKvDB example. It
// picks n consecutive free ports, writes them to the config file and
// starts every node as a child process of this binary:
//
//	kvserver -n 3 -f test.conf
//
// Press Ctrl-C to shut the node or the whole bootstrapped cluster down.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/yukino-js/shita.go/yukino-kv/config"
	"github.com/yukino-js/shita.go/yukino-kv/kvserver"
)

func main() {
	var (
		configFile   = flag.String("f", "test.conf", "node address config file")
		id           = flag.Int("id", -1, "this node's id (single-node mode)")
		nodeNum      = flag.Int("n", 0, "bootstrap a local cluster of n child nodes")
		maxRaftState = flag.Int("maxraftstate", 500, "snapshot when the persisted Raft state exceeds this many bytes (-1 disables)")
		dataDir      = flag.String("datadir", ".", "directory for persisted Raft state and snapshots")
		verbose      = flag.Bool("v", false, "enable debug logging")
	)
	flag.Parse()

	logger := newLogger(*verbose)
	var err error
	switch {
	case *nodeNum > 0 && *id < 0:
		err = runCluster(*configFile, *nodeNum, *maxRaftState, *dataDir, *verbose, logger)
	case *id >= 0:
		err = runNode(*configFile, *id, *maxRaftState, *dataDir, logger)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func newLogger(verbose bool) *slog.Logger {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

// runNode serves a single cluster node until SIGINT/SIGTERM arrives.
func runNode(configFile string, id, maxRaftState int, dataDir string, logger *slog.Logger) error {
	nodes, err := config.Load(configFile)
	if err != nil {
		return err
	}
	if id >= len(nodes) {
		return fmt.Errorf("node id %d out of range: %s defines %d nodes", id, configFile, len(nodes))
	}
	addrs := make([]string, len(nodes))
	for i, n := range nodes {
		addrs[i] = n.Addr()
	}

	node, err := kvserver.NewNode(kvserver.NodeConfig{
		ID:           id,
		Addrs:        addrs,
		MaxRaftState: maxRaftState,
		DataDir:      dataDir,
		Logger:       logger,
	})
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() { errCh <- node.Serve() }()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errCh:
		node.Shutdown()
		return err
	case sig := <-stop:
		logger.Info("signal received; shutting down", "signal", sig.String())
		node.Shutdown()
		return <-errCh
	}
}

// runCluster writes a fresh config file for n local nodes and starts each
// of them as a child process, mirroring the fork-based C++ example.
func runCluster(configFile string, n, maxRaftState int, dataDir string, verbose bool, logger *slog.Logger) error {
	basePort, err := findFreePortRange(n)
	if err != nil {
		return err
	}
	nodes := make([]config.Node, n)
	for i := range nodes {
		nodes[i] = config.Node{IP: "127.0.0.1", Port: basePort + i}
	}
	if err := config.Save(configFile, nodes); err != nil {
		return err
	}
	logger.Info("wrote cluster config", "path", configFile, "basePort", basePort, "nodes", n)

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate own binary: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmds := make([]*exec.Cmd, 0, n)
	for i := 0; i < n; i++ {
		args := []string{
			"-f", configFile,
			"-id", strconv.Itoa(i),
			"-maxraftstate", strconv.Itoa(maxRaftState),
			"-datadir", dataDir,
		}
		if verbose {
			args = append(args, "-v")
		}
		cmd := exec.CommandContext(ctx, exe, args...)
		// Ask children to exit gracefully before the context's default
		// SIGKILL escalation kicks in.
		cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
		cmd.WaitDelay = 5 * time.Second
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			cancel()
			return fmt.Errorf("start node %d: %w", i, err)
		}
		cmds = append(cmds, cmd)
		logger.Info("started node", "id", i, "pid", cmd.Process.Pid, "port", basePort+i)
	}
	logger.Info("cluster is up; press Ctrl-C to stop",
		"nodes", n, "config", configFile)

	// Wait for a signal or for any child to exit unexpectedly.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	childDone := make(chan error, 1)
	go func() { childDone <- waitAll(cmds) }()

	select {
	case <-sig:
		logger.Info("shutting down cluster")
		cancel()
		<-childDone
		return nil
	case err := <-childDone:
		cancel()
		if err != nil {
			return fmt.Errorf("a cluster node exited unexpectedly: %w", err)
		}
		return nil
	}
}

// waitAll waits for every child and reports the first failure.
func waitAll(cmds []*exec.Cmd) error {
	var firstErr error
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil && firstErr == nil && !isSignalKill(err) {
			firstErr = fmt.Errorf("node %d: %w", i, err)
		}
	}
	return firstErr
}

// isSignalKill reports whether err is the expected exit-after-signal
// result of shutting the cluster down.
func isSignalKill(err error) bool {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			return status.Signaled()
		}
	}
	return false
}

// findFreePortRange returns the first port of n consecutive free TCP
// ports on the loopback interface. There is an inherent race between the
// probe and the children binding the ports; for a local demo cluster a
// retry loop keeps it negligible in practice.
func findFreePortRange(n int) (int, error) {
	for attempt := 0; attempt < 100; attempt++ {
		base := 10000 + rand.IntN(20000)
		if portsFree(base, n) {
			return base, nil
		}
	}
	return 0, errors.New("could not find a free port range")
}

func portsFree(base, n int) bool {
	listeners := make([]net.Listener, 0, n)
	defer func() {
		for _, ln := range listeners {
			ln.Close()
		}
	}()
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+i))
		if err != nil {
			return false
		}
		listeners = append(listeners, ln)
	}
	return true
}
