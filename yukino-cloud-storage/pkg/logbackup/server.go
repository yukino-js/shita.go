package logbackup

import (
	"bufio"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/hangtiancheng/yukino.go/libs/yukino_rpc/pkg/rpc"
)

// ServerOptions configures a backup receiver.
type ServerOptions struct {
	// Path is the file the received records are appended to.
	Path string
	// MaxBatch caps how many records a single request may carry.
	MaxBatch int
}

// Server receives log records over RPC and appends them to a file.
type Server struct {
	opts ServerOptions
	rpc  *rpc.Server

	mu   sync.Mutex
	file *os.File
	bw   *bufio.Writer
}

// NewServer builds a receiver. The underlying file is opened lazily on the
// first batch.
func NewServer(opts ServerOptions) *Server {
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = 1024
	}
	return &Server{opts: opts}
}

// Append implements the reflected RPC handler. It returns a non-nil reply
// because the framework cannot encode a nil response body.
func (s *Server) Append(ctx context.Context, req *AppendRequest) (*AppendReply, error) {
	if req == nil || len(req.Records) == 0 {
		return &AppendReply{Stored: 0}, nil
	}
	records := req.Records
	if len(records) > s.opts.MaxBatch {
		records = records[:s.opts.MaxBatch]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	stored := 0
	for _, rec := range records {
		if _, err := s.bw.WriteString(rec); err != nil {
			return &AppendReply{Stored: stored}, err
		}
		// Guarantee one record per line even if a caller omitted the newline.
		if !strings.HasSuffix(rec, "\n") {
			if _, err := s.bw.WriteString("\n"); err != nil {
				return &AppendReply{Stored: stored}, err
			}
		}
		stored++
	}
	if err := s.bw.Flush(); err != nil {
		return &AppendReply{Stored: stored}, err
	}
	return &AppendReply{Stored: stored}, nil
}

func (s *Server) ensureOpen() error {
	if s.file != nil {
		return nil
	}
	if dir := filepath.Dir(s.opts.Path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(s.opts.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	s.file = f
	s.bw = bufio.NewWriter(f)
	return nil
}

// Serve registers the RPC service and blocks serving on lis.
func (s *Server) Serve(lis net.Listener) error {
	s.rpc = rpc.NewServer()
	s.rpc.Register(ServiceName, s)
	return s.rpc.Serve(lis)
}

// ListenAndServe listens on addr and serves until the listener fails.
func (s *Server) ListenAndServe(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(lis)
}

// Stop gracefully stops the RPC server and closes the backing file.
func (s *Server) Stop() {
	if s.rpc != nil {
		s.rpc.GracefulStop()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bw != nil {
		_ = s.bw.Flush()
	}
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
		s.bw = nil
	}
}
