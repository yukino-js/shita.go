// Package config reads and writes the simple node-address file shared by
// every process in a cluster. The format matches the C++ original:
//
//	node0ip=127.0.0.1
//	node0port=29016
//	node1ip=127.0.0.1
//	node1port=29017
//
// Nodes are read in order starting at 0; the first missing node{i}ip ends
// the list. Blank lines and #-comments are ignored.
package config

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// Node is one cluster member's listen address.
type Node struct {
	IP   string
	Port int
}

// Addr renders the node as a "host:port" string ready for net.Listen or
// rpc.Dial.
func (n Node) Addr() string { return net.JoinHostPort(n.IP, strconv.Itoa(n.Port)) }

// Load parses the cluster configuration file at path.
func Load(path string) ([]Node, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config: open %s: %w", path, err)
	}
	defer f.Close()

	values := make(map[string]string)
	scanner := bufio.NewScanner(f)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("config: %s:%d: expected key=value, got %q", path, lineNo, line)
		}
		values[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	var nodes []Node
	for i := 0; ; i++ {
		ipKey := fmt.Sprintf("node%dip", i)
		ip, ok := values[ipKey]
		if !ok || ip == "" {
			break
		}
		portKey := fmt.Sprintf("node%dport", i)
		portStr, ok := values[portKey]
		if !ok {
			return nil, fmt.Errorf("config: %s defines %s but not %s", path, ipKey, portKey)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("config: invalid %s value %q", portKey, portStr)
		}
		nodes = append(nodes, Node{IP: ip, Port: port})
	}
	if len(nodes) == 0 {
		return nil, errors.New("config: no nodes found (expected node0ip/node0port)")
	}
	return nodes, nil
}

// Save writes the cluster configuration file at path, replacing any
// previous content.
func Save(path string, nodes []Node) error {
	var buf strings.Builder
	for i, n := range nodes {
		fmt.Fprintf(&buf, "node%dip=%s\n", i, n.IP)
		fmt.Fprintf(&buf, "node%dport=%d\n", i, n.Port)
	}
	if err := os.WriteFile(path, []byte(buf.String()), 0o644); err != nil {
		return fmt.Errorf("config: write %s: %w", path, err)
	}
	return nil
}
