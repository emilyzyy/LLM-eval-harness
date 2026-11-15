package proxy

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emilyzyy/LLM-eval-harness/gateway/gen/go/capturepb"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/capture"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/config"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/fakeupstream"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/metrics"
)

const (
	proxyKey    = "test-proxy-key"
	upstreamKey = "test-upstream-key"
)

// harness is a gateway wired to an upstream, both running in this process on
// real local sockets.
type harness struct {
	t       *testing.T
	gateway *Gateway
	server  *httptest.Server // the gateway
	sink    *capture.MemorySink
	metrics *metrics.Metrics
	logs    *syncBuffer
}

// newHarness starts a gateway in front of upstreamURL, capturing to an
// in-memory sink (nil turns capture off). adjust, if not nil, may change the
// config before the gateway is built.
func newHarness(t *testing.T, upstreamURL string, sink *capture.MemorySink, adjust func(*config.Config)) *harness {
	t.Helper()
	// Passing a nil *MemorySink straight through would give the gateway a
	// non-nil interface holding a nil pointer, so convert explicitly.
	var captureSink capture.Sink
	if sink != nil {
		captureSink = sink
	}
	h := newHarnessWithSink(t, upstreamURL, metrics.New(), captureSink, adjust)
	h.sink = sink
	return h
}

// newHarnessWithSink is newHarness for any kind of sink. The caller creates
// the metrics so that it can connect a sink's counters to them first.
func newHarnessWithSink(t *testing.T, upstreamURL string, m *metrics.Metrics, sink capture.Sink, adjust func(*config.Config)) *harness {
	t.Helper()

	cfg := config.Default()
	cfg.ProxyKey = proxyKey
	cfg.Upstreams = []config.Upstream{{
		Name:    "fake",
		BaseURL: upstreamURL,
		APIKey:  upstreamKey,
		Models:  []string{"fake-model"},
	}}
	cfg.Retry.Backoff = time.Millisecond
	if adjust != nil {
		adjust(&cfg)
	}

	h := &harness{t: t, metrics: m, logs: &syncBuffer{}}
	logger := slog.New(slog.NewJSONHandler(h.logs, nil))
	h.gateway = New(cfg, sink, h.metrics, logger)
	h.server = httptest.NewServer(h.gateway.Handler())
	t.Cleanup(func() {
		h.server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		h.gateway.Shutdown(ctx)
	})
	return h
}

// startFake starts a fake upstream that insists on the upstream key, which
// proves the gateway swapped the client's proxy key for it.
func startFake(t *testing.T, cfg fakeupstream.Config) (*fakeupstream.Server, string) {
	t.Helper()
	cfg.RequireKey = upstreamKey
	fake := fakeupstream.New(cfg)
	server := httptest.NewServer(fake.Handler())
	t.Cleanup(server.Close)
	return fake, server.URL
}

// startUpstream starts an upstream with custom behaviour.
func startUpstream(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server.URL
}

const chatBody = `{"model":"fake-model","messages":[{"role":"user","content":"hello there"}]}`
const streamBody = `{"model":"fake-model","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hello there"}]}`

// post sends a chat completion request to the gateway with the proxy key.
func (h *harness) post(ctx context.Context, body string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.server.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+proxyKey)
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

// mustPost sends a request and returns the response and its whole body.
func (h *harness) mustPost(body string) (*http.Response, []byte) {
	h.t.Helper()
	resp, err := h.post(context.Background(), body)
	if err != nil {
		h.t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatalf("reading response: %v", err)
	}
	return resp, data
}

// waitRecords waits until the sink holds n capture records and returns them.
// Records are written in the background, so they appear shortly after the
// response rather than with it.
func (h *harness) waitRecords(n int) []*capturepb.CaptureRecord {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		records := h.sink.Records()
		if len(records) >= n {
			return records
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("expected %d capture records, got %d", n, len(records))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// eventually retries check until it returns true or two seconds pass.
func eventually(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// syncBuffer is a bytes.Buffer that several goroutines can write to.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
