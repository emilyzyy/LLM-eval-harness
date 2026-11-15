package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/proto"

	"github.com/emilyzyy/LLM-eval-harness/gateway/gen/go/capturepb"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/capture"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/config"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/fakeupstream"
)

func TestNonStreaming(t *testing.T) {
	fake, url := startFake(t, fakeupstream.Config{Tokens: 4, Latency: 20 * time.Millisecond})
	h := newHarness(t, url, &capture.MemorySink{}, nil)

	resp, body := h.mustPost(chatBody)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	var completion struct {
		Choices []struct {
			Message struct{ Content string }
		}
	}
	if err := json.Unmarshal(body, &completion); err != nil || len(completion.Choices) != 1 {
		t.Fatalf("response is not a chat completion: %s", body)
	}
	if got := completion.Choices[0].Message.Content; got != "tok0 tok1 tok2 tok3" {
		t.Errorf("content = %q", got)
	}
	if fake.Requests() != 1 {
		t.Errorf("upstream saw %d requests, want 1", fake.Requests())
	}

	// The request ID is a UUIDv7 and is returned in a header.
	requestID := resp.Header.Get("X-Request-ID")
	parsed, err := uuid.Parse(requestID)
	if err != nil || parsed.Version() != 7 {
		t.Errorf("X-Request-ID %q is not a UUIDv7", requestID)
	}

	rec := h.waitRecords(1)[0]
	if rec.RequestId != requestID {
		t.Errorf("record request_id = %q, header = %q", rec.RequestId, requestID)
	}
	if rec.Model != "fake-model" || rec.Upstream != "fake" || rec.Stream {
		t.Errorf("record model/upstream/stream = %q/%q/%v", rec.Model, rec.Upstream, rec.Stream)
	}
	if string(rec.RequestBody) != chatBody {
		t.Errorf("record request body = %s", rec.RequestBody)
	}
	if !bytes.Equal(rec.ResponseBody, body) {
		t.Errorf("record response body differs from what the client received")
	}
	if rec.StatusCode != 200 || rec.Outcome != capturepb.Outcome_OUTCOME_COMPLETED || rec.Attempts != 1 {
		t.Errorf("record status/outcome/attempts = %d/%v/%d", rec.StatusCode, rec.Outcome, rec.Attempts)
	}
	if rec.Usage.GetPromptTokens() != 2 || rec.Usage.GetCompletionTokens() != 4 || rec.Usage.GetTotalTokens() != 6 {
		t.Errorf("record usage = %v", rec.Usage)
	}
	upstream, total := rec.UpstreamLatency.AsDuration(), rec.TotalLatency.AsDuration()
	if upstream < 20*time.Millisecond || total < upstream {
		t.Errorf("latencies: upstream = %v, total = %v", upstream, total)
	}
	if !rec.CompletedAt.AsTime().After(rec.ReceivedAt.AsTime()) {
		t.Errorf("completed_at is not after received_at")
	}
}

func TestRequestIDFromClient(t *testing.T) {
	_, url := startFake(t, fakeupstream.Config{})
	h := newHarness(t, url, &capture.MemorySink{}, nil)

	send := func(id string) string {
		req, _ := http.NewRequest(http.MethodPost, h.server.URL+"/v1/chat/completions", strings.NewReader(chatBody))
		req.Header.Set("Authorization", "Bearer "+proxyKey)
		req.Header.Set("X-Request-ID", id)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.Header.Get("X-Request-ID")
	}

	if got := send("client-id-123"); got != "client-id-123" {
		t.Errorf("a valid client ID was not kept: got %q", got)
	}
	// An ID with characters that could corrupt a log line is replaced.
	if got := send("bad id\twith spaces"); got == "bad id\twith spaces" || uuid.Validate(got) != nil {
		t.Errorf("an unsafe client ID was not replaced: got %q", got)
	}
	if rec := h.waitRecords(2)[0]; rec.RequestId != "client-id-123" {
		t.Errorf("record request_id = %q", rec.RequestId)
	}
}

func TestAuth(t *testing.T) {
	fake, url := startFake(t, fakeupstream.Config{})
	h := newHarness(t, url, &capture.MemorySink{}, nil)

	for _, header := range []string{"", "Bearer wrong", "Bearer " + upstreamKey, proxyKey} {
		req, _ := http.NewRequest(http.MethodPost, h.server.URL+"/v1/chat/completions", strings.NewReader(chatBody))
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("Authorization %q: status = %d, want 401", header, resp.StatusCode)
		}
	}
	if fake.Requests() != 0 {
		t.Errorf("unauthenticated requests reached the upstream")
	}
}

func TestBadRequests(t *testing.T) {
	_, url := startFake(t, fakeupstream.Config{})
	h := newHarness(t, url, &capture.MemorySink{}, func(cfg *config.Config) {
		cfg.MaxBodyBytes = 200
	})

	cases := []struct {
		name string
		body string
		want int
	}{
		{"not JSON", `{"model":`, http.StatusBadRequest},
		{"unknown model", `{"model":"other"}`, http.StatusNotFound},
		{"too large", `{"model":"fake-model","pad":"` + strings.Repeat("x", 300) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for _, c := range cases {
		resp, _ := h.mustPost(c.body)
		if resp.StatusCode != c.want {
			t.Errorf("%s: status = %d, want %d", c.name, resp.StatusCode, c.want)
		}
	}
}

// TestStreamingIsIncremental checks that chunks reach the client as the
// upstream produces them. The fake takes 400ms to send five chunks; if the
// gateway buffered the stream, they would all arrive together at the end.
func TestStreamingIsIncremental(t *testing.T) {
	const chunkDelay = 100 * time.Millisecond
	_, url := startFake(t, fakeupstream.Config{Tokens: 5, ChunkDelay: chunkDelay})
	h := newHarness(t, url, &capture.MemorySink{}, nil)

	resp, err := h.post(context.Background(), streamBody)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q", ct)
	}

	// Note when each content chunk arrives.
	var received bytes.Buffer
	var arrivals []time.Time
	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadString('\n')
		received.WriteString(line)
		if strings.Contains(line, `"content"`) {
			arrivals = append(arrivals, time.Now())
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading stream: %v", err)
		}
	}

	if len(arrivals) != 5 {
		t.Fatalf("got %d content chunks, want 5", len(arrivals))
	}
	for i := 1; i < len(arrivals); i++ {
		if gap := arrivals[i].Sub(arrivals[i-1]); gap < chunkDelay/2 {
			t.Errorf("chunk %d arrived %v after chunk %d; chunks are being buffered", i, gap, i-1)
		}
	}
	if !strings.HasSuffix(received.String(), "data: [DONE]\n\n") {
		t.Errorf("stream did not end with [DONE]")
	}

	rec := h.waitRecords(1)[0]
	if !rec.Stream || rec.Outcome != capturepb.Outcome_OUTCOME_COMPLETED || rec.StatusCode != 200 {
		t.Errorf("record stream/outcome/status = %v/%v/%d", rec.Stream, rec.Outcome, rec.StatusCode)
	}
	if !bytes.Equal(rec.ResponseBody, received.Bytes()) {
		t.Errorf("record response body differs from the stream the client received")
	}
	if rec.Usage.GetCompletionTokens() != 5 {
		t.Errorf("record usage = %v, want 5 completion tokens", rec.Usage)
	}
}

func TestClientCancelStopsUpstream(t *testing.T) {
	fake, url := startFake(t, fakeupstream.Config{Latency: 10 * time.Second})
	h := newHarness(t, url, &capture.MemorySink{}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Cancel once the upstream has started working on the request.
		for fake.Requests() == 0 {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()
	_, err := h.post(ctx, chatBody)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	// The upstream was going to work for 10 seconds. It must see the
	// cancellation almost at once.
	eventually(t, "upstream call to be cancelled", func() bool { return fake.Cancelled() == 1 })

	rec := h.waitRecords(1)[0]
	if rec.Outcome != capturepb.Outcome_OUTCOME_CLIENT_CANCELLED || rec.StatusCode != 0 {
		t.Errorf("record outcome/status = %v/%d", rec.Outcome, rec.StatusCode)
	}
	if got := testutil.ToFloat64(h.metrics.Requests.WithLabelValues("499")); got != 1 {
		t.Errorf("requests_total{status=499} = %v, want 1", got)
	}
}

func TestClientCancelMidStream(t *testing.T) {
	fake, url := startFake(t, fakeupstream.Config{Tokens: 1000, ChunkDelay: 20 * time.Millisecond})
	h := newHarness(t, url, &capture.MemorySink{}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp, err := h.post(ctx, streamBody)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
		t.Fatalf("reading first chunk: %v", err)
	}
	cancel()

	// The full stream would take 20 seconds.
	eventually(t, "upstream stream to be cancelled", func() bool { return fake.Cancelled() == 1 })
	rec := h.waitRecords(1)[0]
	if rec.Outcome != capturepb.Outcome_OUTCOME_CLIENT_CANCELLED {
		t.Errorf("record outcome = %v", rec.Outcome)
	}
	if len(rec.ResponseBody) == 0 {
		t.Errorf("record should hold the part of the stream that was sent")
	}
}

func TestUpstreamErrorIsRetriedThenPassedThrough(t *testing.T) {
	fake, url := startFake(t, fakeupstream.Config{ErrorRate: 1})
	h := newHarness(t, url, &capture.MemorySink{}, nil)

	resp, body := h.mustPost(chatBody)

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want the upstream's 503", resp.StatusCode)
	}
	if !strings.Contains(string(body), "injected failure") {
		t.Errorf("the upstream's error body was not passed through: %s", body)
	}
	if fake.Requests() != 2 {
		t.Errorf("upstream saw %d attempts, want 2", fake.Requests())
	}
	rec := h.waitRecords(1)[0]
	if rec.StatusCode != 503 || rec.Attempts != 2 || rec.Outcome != capturepb.Outcome_OUTCOME_COMPLETED {
		t.Errorf("record status/attempts/outcome = %d/%d/%v", rec.StatusCode, rec.Attempts, rec.Outcome)
	}
}

func TestRetryRecoversFromOneFailure(t *testing.T) {
	var calls atomic.Int64
	url := startUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, `{"choices":[]}`)
	})
	h := newHarness(t, url, &capture.MemorySink{}, nil)

	resp, _ := h.mustPost(chatBody)

	if resp.StatusCode != http.StatusOK || calls.Load() != 2 {
		t.Errorf("status = %d after %d upstream calls, want 200 after 2", resp.StatusCode, calls.Load())
	}
}

func TestClientErrorsAreNotRetried(t *testing.T) {
	var calls atomic.Int64
	url := startUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "bad request", http.StatusBadRequest)
	})
	h := newHarness(t, url, &capture.MemorySink{}, nil)

	resp, _ := h.mustPost(chatBody)

	if resp.StatusCode != http.StatusBadRequest || calls.Load() != 1 {
		t.Errorf("status = %d after %d upstream calls, want 400 after 1", resp.StatusCode, calls.Load())
	}
}

func TestUpstreamUnreachable(t *testing.T) {
	// Start an upstream and close it, so its address refuses connections.
	closed := httptest.NewServer(http.NotFoundHandler())
	url := closed.URL
	closed.Close()
	h := newHarness(t, url, &capture.MemorySink{}, nil)

	resp, body := h.mustPost(chatBody)

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	// The client must not be shown the internal address that failed.
	if strings.Contains(string(body), "127.0.0.1") {
		t.Errorf("error body leaks the upstream address: %s", body)
	}
	rec := h.waitRecords(1)[0]
	if rec.Outcome != capturepb.Outcome_OUTCOME_UPSTREAM_ERROR || rec.StatusCode != 502 || rec.Attempts != 2 {
		t.Errorf("record outcome/status/attempts = %v/%d/%d", rec.Outcome, rec.StatusCode, rec.Attempts)
	}
}

func TestUpstreamTimeout(t *testing.T) {
	fake, url := startFake(t, fakeupstream.Config{Latency: 10 * time.Second})
	h := newHarness(t, url, &capture.MemorySink{}, func(cfg *config.Config) {
		cfg.Timeouts.Request = 100 * time.Millisecond
	})

	start := time.Now()
	resp, _ := h.mustPost(chatBody)
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", resp.StatusCode)
	}
	if elapsed > 2*time.Second {
		t.Errorf("timeout took %v, configured for 100ms", elapsed)
	}
	// A timeout is not retried: the client has already waited its limit.
	if fake.Requests() != 1 {
		t.Errorf("upstream saw %d attempts, want 1", fake.Requests())
	}
	rec := h.waitRecords(1)[0]
	if rec.Outcome != capturepb.Outcome_OUTCOME_UPSTREAM_TIMEOUT || rec.StatusCode != 504 {
		t.Errorf("record outcome/status = %v/%d", rec.Outcome, rec.StatusCode)
	}
}

func TestResponseHeaderTimeout(t *testing.T) {
	_, url := startFake(t, fakeupstream.Config{Latency: 10 * time.Second})
	h := newHarness(t, url, &capture.MemorySink{}, func(cfg *config.Config) {
		cfg.Timeouts.ResponseHeader = 100 * time.Millisecond
	})

	// A streaming request has no overall deadline, so this exercises the
	// limit on how long the upstream may take to start answering.
	resp, _ := h.mustPost(streamBody)
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", resp.StatusCode)
	}
}

// TestStreamIdleTimeout uses an upstream that sends one chunk and then goes
// silent. The gateway must give up and cut the connection, so the client can
// tell the stream is incomplete.
func TestStreamIdleTimeout(t *testing.T) {
	url := startUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[]}\n\n")
		http.NewResponseController(w).Flush()
		<-r.Context().Done()
	})
	h := newHarness(t, url, &capture.MemorySink{}, func(cfg *config.Config) {
		cfg.Timeouts.StreamIdle = 100 * time.Millisecond
	})

	resp, err := h.post(context.Background(), streamBody)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Errorf("the stream ended cleanly; want an error because it was cut off")
	}

	rec := h.waitRecords(1)[0]
	if rec.Outcome != capturepb.Outcome_OUTCOME_UPSTREAM_TIMEOUT {
		t.Errorf("record outcome = %v", rec.Outcome)
	}
	if string(rec.ResponseBody) != "data: {\"choices\":[]}\n\n" {
		t.Errorf("record response body = %q", rec.ResponseBody)
	}
}

func TestUpstreamDiesMidStream(t *testing.T) {
	url := startUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[]}\n\n")
		http.NewResponseController(w).Flush()
		panic(http.ErrAbortHandler) // drop the connection
	})
	h := newHarness(t, url, &capture.MemorySink{}, nil)

	resp, err := h.post(context.Background(), streamBody)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Errorf("the stream ended cleanly; want an error because it was cut off")
	}
	rec := h.waitRecords(1)[0]
	if rec.Outcome != capturepb.Outcome_OUTCOME_UPSTREAM_ERROR || rec.Attempts != 1 {
		t.Errorf("record outcome/attempts = %v/%d", rec.Outcome, rec.Attempts)
	}
}

func TestRateLimit(t *testing.T) {
	fake, url := startFake(t, fakeupstream.Config{})
	h := newHarness(t, url, &capture.MemorySink{}, func(cfg *config.Config) {
		// Two requests may pass at once; refill is far slower than the test.
		cfg.Upstreams[0].RateLimit = config.RateLimit{RequestsPerSecond: 0.1, Burst: 2}
	})

	var statuses []int
	var last *http.Response
	for range 3 {
		last, _ = h.mustPost(chatBody)
		statuses = append(statuses, last.StatusCode)
	}

	if fmt.Sprint(statuses) != "[200 200 429]" {
		t.Fatalf("statuses = %v, want [200 200 429]", statuses)
	}
	if last.Header.Get("Retry-After") != "10" {
		t.Errorf("Retry-After = %q, want 10", last.Header.Get("Retry-After"))
	}
	if fake.Requests() != 2 {
		t.Errorf("upstream saw %d requests, want 2", fake.Requests())
	}
	if got := testutil.ToFloat64(h.metrics.RateLimited.WithLabelValues("fake")); got != 1 {
		t.Errorf("rate_limited_total = %v, want 1", got)
	}
	// A request the gateway turned away never reached a model, so there is
	// nothing to capture.
	time.Sleep(50 * time.Millisecond)
	if n := len(h.sink.Records()); n != 2 {
		t.Errorf("%d capture records, want 2", n)
	}
}

// TestCaptureOverflowDropsWithoutSlowingResponses gives the gateway a sink
// that takes 200ms per record and a queue with room for two. Twenty requests
// would take four seconds if any of them waited for the sink.
func TestCaptureOverflowDropsWithoutSlowingResponses(t *testing.T) {
	_, url := startFake(t, fakeupstream.Config{})
	sink := &capture.MemorySink{Delay: 200 * time.Millisecond}
	h := newHarness(t, url, sink, func(cfg *config.Config) {
		cfg.Capture.QueueSize = 2
	})

	const requests = 20
	start := time.Now()
	for range requests {
		requestStart := time.Now()
		resp, _ := h.mustPost(chatBody)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if took := time.Since(requestStart); took > 150*time.Millisecond {
			t.Errorf("a request took %v; it waited for the sink", took)
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("%d requests took %v", requests, elapsed)
	}

	// Every record was either queued or dropped. The gateway does this just
	// after the response is sent, so the last one may still be in progress.
	eventually(t, "every record to be queued or dropped", func() bool {
		enqueued := testutil.ToFloat64(h.metrics.CaptureEnqueued)
		dropped := testutil.ToFloat64(h.metrics.CaptureDropped)
		return enqueued+dropped == requests
	})
	if dropped := testutil.ToFloat64(h.metrics.CaptureDropped); dropped < requests/2 {
		t.Errorf("only %v records were dropped; the queue should have overflowed", dropped)
	}
}

func TestSinkFailureDoesNotAffectResponses(t *testing.T) {
	_, url := startFake(t, fakeupstream.Config{})
	sink := &capture.MemorySink{Err: errors.New("disk full")}
	h := newHarness(t, url, sink, nil)

	const requests = 10
	for range requests {
		resp, body := h.mustPost(chatBody)
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "choices") {
			t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
		}
	}
	eventually(t, "every sink error to be counted", func() bool {
		return testutil.ToFloat64(h.metrics.CaptureSinkError) == requests
	})
}

func TestCaptureBodyLimit(t *testing.T) {
	_, url := startFake(t, fakeupstream.Config{Tokens: 200})
	h := newHarness(t, url, &capture.MemorySink{}, func(cfg *config.Config) {
		cfg.Capture.MaxBodyBytes = 50
	})

	_, body := h.mustPost(chatBody)

	rec := h.waitRecords(1)[0]
	if !rec.RequestTruncated || len(rec.RequestBody) != 50 {
		t.Errorf("request: truncated = %v, len = %d", rec.RequestTruncated, len(rec.RequestBody))
	}
	if !rec.ResponseTruncated || !bytes.Equal(rec.ResponseBody, body[:50]) {
		t.Errorf("response: truncated = %v, len = %d", rec.ResponseTruncated, len(rec.ResponseBody))
	}
	if rec.Usage != nil {
		t.Errorf("usage should be unset when the body it would be read from was cut")
	}
}

// TestPlainResponseOverBufferLimit sends a plain response longer than
// max_response_buffer_bytes. The upstream sends the first part, then holds the
// rest back until the client has received that first part. A gateway that
// still buffered the whole response would send nothing, and the test would
// time out. The client must then get the whole body, and the record only the
// part that was buffered, marked truncated.
func TestPlainResponseOverBufferLimit(t *testing.T) {
	const bufferLimit = 1000
	head := bytes.Repeat([]byte("a"), 3*bufferLimit)
	tail := bytes.Repeat([]byte("b"), 3*bufferLimit)
	release := make(chan struct{})
	url := startUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", fmt.Sprint(len(head)+len(tail)))
		w.Write(head)
		w.(http.Flusher).Flush()
		select {
		case <-release:
			w.Write(tail)
		case <-r.Context().Done():
		}
	})
	h := newHarness(t, url, &capture.MemorySink{}, func(cfg *config.Config) {
		cfg.MaxResponseBufferBytes = bufferLimit
		// Above the buffer limit, so the buffer limit is what cuts the record.
		cfg.Capture.MaxBodyBytes = 10 * bufferLimit
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var resp *http.Response
	got := make([]byte, len(head))
	readHead := make(chan error, 1)
	go func() {
		var err error
		resp, err = h.post(ctx, chatBody)
		if err == nil {
			_, err = io.ReadFull(resp.Body, got)
		}
		readHead <- err
	}()
	select {
	case err := <-readHead:
		if err != nil {
			t.Fatalf("receiving the first part: %v", err)
		}
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("the client got nothing while the upstream held back the rest: the gateway buffered the whole response")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ContentLength != int64(len(head)+len(tail)) {
		t.Errorf("status = %d, content length = %d", resp.StatusCode, resp.ContentLength)
	}
	close(release)
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the rest: %v", err)
	}
	if !bytes.Equal(append(got, rest...), append(head, tail...)) {
		t.Errorf("the client got %d bytes, not the full %d-byte body", len(got)+len(rest), len(head)+len(tail))
	}

	rec := h.waitRecords(1)[0]
	if !rec.ResponseTruncated || !bytes.Equal(rec.ResponseBody, head[:bufferLimit]) {
		t.Errorf("record: truncated = %v, body length = %d, want the first %d bytes", rec.ResponseTruncated, len(rec.ResponseBody), bufferLimit)
	}
	if rec.StatusCode != http.StatusOK || rec.Outcome != capturepb.Outcome_OUTCOME_COMPLETED {
		t.Errorf("record: status = %d, outcome = %v", rec.StatusCode, rec.Outcome)
	}
	logs := h.logs.String()
	if !strings.Contains(logs, fmt.Sprintf(`"response_bytes":%d`, len(head)+len(tail))) {
		t.Errorf("the log line should report the full response size; logs: %s", logs)
	}
}

// TestKeysAreNeverLoggedOrCaptured sends requests that succeed, fail and are
// rejected, then searches everything the gateway wrote for either key.
func TestKeysAreNeverLoggedOrCaptured(t *testing.T) {
	_, url := startFake(t, fakeupstream.Config{})
	h := newHarness(t, url, &capture.MemorySink{}, nil)

	h.mustPost(chatBody)
	h.mustPost(streamBody)
	h.mustPost(`{"model":"other"}`)

	for _, rec := range h.waitRecords(2) {
		encoded, err := proto.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(encoded, []byte(proxyKey)) || bytes.Contains(encoded, []byte(upstreamKey)) {
			t.Errorf("a capture record contains a key")
		}
	}
	logs := h.logs.String()
	if logs == "" {
		t.Fatalf("no logs were written")
	}
	if strings.Contains(logs, proxyKey) || strings.Contains(logs, upstreamKey) {
		t.Errorf("the logs contain a key")
	}
}

func TestLogsCarryRequestID(t *testing.T) {
	_, url := startFake(t, fakeupstream.Config{})
	h := newHarness(t, url, &capture.MemorySink{}, nil)

	resp, _ := h.mustPost(chatBody)

	requestID := resp.Header.Get("X-Request-ID")
	eventually(t, "the request to be logged", func() bool {
		return strings.Contains(h.logs.String(), `"request_id":"`+requestID+`"`)
	})
}

func TestHealthAndMetricsEndpoints(t *testing.T) {
	_, url := startFake(t, fakeupstream.Config{})
	h := newHarness(t, url, &capture.MemorySink{}, func(cfg *config.Config) {
		cfg.Upstreams[0].RateLimit = config.RateLimit{RequestsPerSecond: 0.1, Burst: 1}
	})
	h.mustPost(chatBody)
	h.mustPost(chatBody) // rate limited

	get := func(path string) (int, string) {
		resp, err := http.Get(h.server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}

	if status, body := get("/healthz"); status != 200 || !strings.Contains(body, "ok") {
		t.Errorf("/healthz = %d %s", status, body)
	}
	if status, body := get("/readyz"); status != 200 || !strings.Contains(body, `"status":"ready"`) {
		t.Errorf("/readyz = %d %s", status, body)
	}

	_, text := get("/metrics")
	for _, want := range []string{
		`gateway_requests_total{status="200"} 1`,
		`gateway_requests_total{status="429"} 1`,
		`gateway_rate_limited_total{upstream="fake"} 1`,
		"gateway_request_duration_seconds_bucket",
		"gateway_upstream_duration_seconds_bucket",
		"gateway_proxy_overhead_seconds_bucket",
		"gateway_in_flight_requests 0",
		"gateway_capture_dropped_total 0",
		"gateway_kafka_produce_errors_total 0",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("/metrics is missing %q", want)
		}
	}
}

func TestCaptureOff(t *testing.T) {
	_, url := startFake(t, fakeupstream.Config{})
	h := newHarness(t, url, nil, nil)

	resp, _ := h.mustPost(chatBody)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d", resp.StatusCode)
	}
}
