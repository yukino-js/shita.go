package storage

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yukino-js/shita.go/yukino-cloud-storage/pkg/asynclog"
)

func newTestService(t *testing.T) (*Service, *Config) {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DeepStorageDir = filepath.Join(dir, "deep") + string(filepath.Separator)
	cfg.LowStorageDir = filepath.Join(dir, "low") + string(filepath.Separator)
	cfg.StorageInfoFile = filepath.Join(dir, "storage.data")
	cfg.TempDir = filepath.Join(dir, "tmp")
	cfg.DownloadPrefix = "/download/"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	data, err := NewManager(cfg.StorageInfoFile)
	if err != nil {
		t.Fatal(err)
	}
	logger, err := asynclog.NewBuilder().
		Name("test-" + t.Name()).
		Flusher(asynclog.NewStdoutFlusher(asynclog.FlushNone)).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logger.Close() })
	svc, err := NewService(cfg, data, logger)
	if err != nil {
		t.Fatal(err)
	}
	return svc, cfg
}

func upload(t *testing.T, srv *httptest.Server, name string, content []byte, tier string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/upload", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("FileName", base64.StdEncoding.EncodeToString([]byte(name)))
	req.Header.Set("StorageType", tier)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestUploadDownloadLow(t *testing.T) {
	svc, _ := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	content := []byte("hello low storage")
	resp := upload(t, srv, "hello.txt", content, "low")
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("upload status %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	dresp, err := http.Get(srv.URL + "/download/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(dresp.Body)
	dresp.Body.Close()
	if dresp.StatusCode != http.StatusOK {
		t.Fatalf("download status %d", dresp.StatusCode)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("download mismatch: got %q want %q", got, content)
	}
}

func TestUploadDownloadDeep(t *testing.T) {
	svc, cfg := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	content := bytes.Repeat([]byte("deep storage payload "), 500)
	resp := upload(t, srv, "deep.bin", content, "deep")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("upload status %d", resp.StatusCode)
	}
	resp.Body.Close()

	// The stored object must be compressed and smaller than the original.
	stored := filepath.Join(cfg.DeepStorageDir, "deep.bin")
	st, err := os.Stat(stored)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() >= int64(len(content)) {
		t.Fatalf("deep storage did not compress: %d >= %d", st.Size(), len(content))
	}

	dresp, err := http.Get(srv.URL + "/download/deep.bin")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(dresp.Body)
	dresp.Body.Close()
	if dresp.StatusCode != http.StatusOK {
		t.Fatalf("download status %d", dresp.StatusCode)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("deep roundtrip mismatch")
	}
}

func TestDownloadRange(t *testing.T) {
	svc, _ := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	content := []byte("0123456789")
	resp := upload(t, srv, "range.txt", content, "low")
	resp.Body.Close()

	rreq, err := http.NewRequest(http.MethodGet, srv.URL+"/download/range.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	rreq.Header.Set("Range", "bytes=2-5")
	rresp, err := http.DefaultClient.Do(rreq)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rresp.Body)
	rresp.Body.Close()
	if rresp.StatusCode != http.StatusPartialContent {
		t.Fatalf("expected 206, got %d", rresp.StatusCode)
	}
	if string(got) != "2345" {
		t.Fatalf("range body = %q, want %q", got, "2345")
	}
}

func TestListPage(t *testing.T) {
	svc, _ := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	resp := upload(t, srv, "listed.txt", []byte("list me"), "low")
	resp.Body.Close()

	lresp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(lresp.Body)
	lresp.Body.Close()
	if lresp.StatusCode != http.StatusOK {
		t.Fatalf("list status %d", lresp.StatusCode)
	}
	if ct := lresp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("content type = %q", ct)
	}
	if !strings.Contains(string(body), "listed.txt") {
		t.Fatalf("list page missing file: %s", body)
	}
}

func TestDownloadNotFound(t *testing.T) {
	svc, _ := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/download/nope.txt")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestUploadSanitisesTraversal(t *testing.T) {
	svc, cfg := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	resp := upload(t, srv, "../../evil.txt", []byte("payload"), "low")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected sanitised upload to succeed, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// The file must land inside the low storage directory, never outside it.
	if !Exists(filepath.Join(cfg.LowStorageDir, "evil.txt")) {
		t.Fatal("sanitised file not stored in the low storage directory")
	}
	parent := filepath.Dir(strings.TrimSuffix(cfg.LowStorageDir, string(filepath.Separator)))
	if Exists(filepath.Join(parent, "evil.txt")) {
		t.Fatal("path traversal escaped the storage directory")
	}
}

func TestUploadRejectsBadStorageType(t *testing.T) {
	svc, _ := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()
	resp := upload(t, srv, "x.txt", []byte("data"), "bogus")
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestSafeFileName(t *testing.T) {
	if got, err := SafeFileName("dir/sub/report.pdf"); err != nil || got != "report.pdf" {
		t.Fatalf("SafeFileName = %q, %v", got, err)
	}
	for _, bad := range []string{"", ".", "..", "   "} {
		if _, err := SafeFileName(bad); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	}
}

func TestSafeJoinStaysInside(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.txt", "../evil", `..\evil`, "/abs/path.txt", "sub/dir/x", "....//x"} {
		got, err := SafeJoin(dir, name)
		if err != nil {
			continue
		}
		rel, rerr := filepath.Rel(dir, got)
		if rerr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			t.Fatalf("name %q escaped to %q", name, got)
		}
	}
}

func TestWriteFileAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "data.json")
	if err := WriteFileAtomic(path, []byte(`{"a":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if string(got) != `{"a":1}` {
		t.Fatalf("content = %q", got)
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".yukino-tmp-") {
			t.Fatalf("leftover temp file %s", e.Name())
		}
	}
}

func TestManagerPersistAndReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "storage.data")
	m, err := NewManager(path)
	if err != nil {
		t.Fatal(err)
	}
	info := &StorageInfo{ModTime: 1, AccessTime: 1, Size: 10, StoragePath: "/x/file.txt", URL: "/download/file.txt", Name: "file.txt"}
	if err := m.Insert(info); err != nil {
		t.Fatal(err)
	}
	if got, ok := m.GetByURL("/download/file.txt"); !ok || got.Name != "file.txt" {
		t.Fatalf("GetByURL = %v, %v", got, ok)
	}
	m2, err := NewManager(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := m2.GetByURL("/download/file.txt")
	if !ok || got.StoragePath != "/x/file.txt" {
		t.Fatal("reload from disk failed")
	}
	if len(m2.All()) != 1 {
		t.Fatalf("All() returned %d records", len(m2.All()))
	}
	if err := m2.Delete("/download/file.txt"); err != nil {
		t.Fatal(err)
	}
	if _, ok := m2.GetByURL("/download/file.txt"); ok {
		t.Fatal("record still present after delete")
	}
}

func TestFormatSize(t *testing.T) {
	cases := map[int64]string{0: "0 B", 512: "512 B", 1024: "1.00 KB", 1536: "1.50 KB", 1048576: "1.00 MB"}
	for in, want := range cases {
		if got := formatSize(in); got != want {
			t.Fatalf("formatSize(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestDownloadHead(t *testing.T) {
	svc, _ := newTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	content := []byte("head request payload")
	resp := upload(t, srv, "head.txt", content, "low")
	resp.Body.Close()

	hresp, err := http.Head(srv.URL + "/download/head.txt")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(hresp.Body)
	hresp.Body.Close()
	if hresp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD status = %d, want 200", hresp.StatusCode)
	}
	if len(body) != 0 {
		t.Fatalf("HEAD returned a body of %d bytes", len(body))
	}
	if hresp.ContentLength != int64(len(content)) {
		t.Fatalf("HEAD Content-Length = %d, want %d", hresp.ContentLength, len(content))
	}
	if hresp.Header.Get("ETag") == "" {
		t.Fatal("HEAD response is missing the ETag header")
	}
}

func TestConfigNormalisesDownloadPrefix(t *testing.T) {
	for in, want := range map[string]string{
		"download":  "/download/",
		"/download": "/download/",
		"download/": "/download/",
		"":          "/download/",
	} {
		c := DefaultConfig()
		c.DownloadPrefix = in
		if err := c.Validate(); err != nil {
			t.Fatalf("Validate(%q): %v", in, err)
		}
		if c.DownloadPrefix != want {
			t.Fatalf("DownloadPrefix(%q) = %q, want %q", in, c.DownloadPrefix, want)
		}
	}
}
