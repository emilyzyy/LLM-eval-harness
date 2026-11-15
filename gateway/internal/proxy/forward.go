package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/emilyzyy/LLM-eval-harness/gateway/gen/go/capturepb"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/upstream"
)

// result is what forward reports back about one request.
type result struct {
	// HTTP status sent to the client, or 0 if the client left first.
	status  int
	outcome capturepb.Outcome
	// Copy of the response body for the capture record, cut at the capture
	// limit. bodyBytes is the full length sent.
	body      []byte
	bodyBytes int
	truncated bool
	// True if body is a server-sent event stream.
	sse bool
	// Time spent waiting on the upstream.
	upstreamTime time.Duration
	attempts     int
	// True if the response must be cut off rather than ended cleanly.
	abort bool
}

// forward sends the request to the upstream and relays the response.
func (g *Gateway) forward(w http.ResponseWriter, r *http.Request, rt *route, body []byte, stream bool, requestID string) result {
	var res result

	// ctx is cancelled when the client disconnects, because it is derived
	// from r.Context(). Cancelling it aborts the upstream call.
	//
	// A non-streaming request gets one deadline for everything. A stream can
	// legitimately run for minutes, so it has no overall deadline; it is
	// cancelled if the upstream goes quiet for too long (see idle below).
	var ctx context.Context
	var cancel context.CancelFunc
	if stream {
		ctx, cancel = context.WithCancel(r.Context())
	} else {
		ctx, cancel = context.WithTimeout(r.Context(), g.cfg.Timeouts.Request)
	}
	defer cancel()

	// --- Call the upstream, retrying while nothing has reached the client.
	req := upstream.Request{Body: body, RequestID: requestID, Stream: stream}
	var resp *http.Response
	var err error
	for {
		res.attempts++
		callStart := time.Now()
		resp, err = rt.upstream.ChatCompletions(ctx, req)
		res.upstreamTime += time.Since(callStart)

		if !shouldRetry(resp, err) || res.attempts >= g.cfg.Retry.MaxAttempts || ctx.Err() != nil {
			break
		}
		if resp != nil {
			resp.Body.Close()
			resp = nil
		}
		// The backoff counts as upstream time: the upstream caused it.
		backoffStart := time.Now()
		if !sleep(ctx, g.cfg.Retry.Backoff) {
			err = ctx.Err()
			break
		}
		res.upstreamTime += time.Since(backoffStart)
	}
	if err != nil {
		g.fail(w, r, &res, err, false)
		return res
	}
	defer resp.Body.Close()

	// idle cancels ctx if the upstream sends nothing for StreamIdle. It is
	// only armed for streaming requests and is reset on every chunk.
	var idleFired atomic.Bool
	var idle *time.Timer
	if stream {
		idle = time.AfterFunc(g.cfg.Timeouts.StreamIdle, func() {
			idleFired.Store(true)
			cancel()
		})
		defer idle.Stop()
	}

	isEventStream := strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")
	if !stream || !isEventStream {
		// --- A plain response: read all of it, then send it in one go.
		// (A streaming request also lands here when the upstream answers
		// with a JSON error instead of a stream.) Holding the whole body
		// lets the client get a Content-Length, and turns an upstream
		// failure part-way through into a clean 502 or 504. Only up to
		// MaxResponseBufferBytes is held; a longer body is passed through.
		limit := g.cfg.MaxResponseBufferBytes
		readStart := time.Now()
		data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
		res.upstreamTime += time.Since(readStart)
		if err != nil {
			g.fail(w, r, &res, err, idleFired.Load())
			return res
		}
		if int64(len(data)) > limit {
			g.passThrough(w, r, resp, data, int(limit), &res, &idleFired)
			return res
		}

		copyHeaders(w.Header(), resp.Header)
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(resp.StatusCode)
		_, writeErr := w.Write(data)
		// Push the bytes out now, so the bookkeeping that follows in
		// handleChat does not delay the client.
		http.NewResponseController(w).Flush()

		res.status = resp.StatusCode
		res.outcome = capturepb.Outcome_OUTCOME_COMPLETED
		if writeErr != nil {
			res.outcome = capturepb.Outcome_OUTCOME_CLIENT_CANCELLED
		}
		res.keep(data, g.cfg.Capture.MaxBodyBytes)
		return res
	}

	// --- A stream: send each chunk on as soon as it arrives.
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	flusher := http.NewResponseController(w)
	flusher.Flush() // send the headers now, so the client knows the stream has begun

	res.status = resp.StatusCode
	res.sse = true
	buf := make([]byte, 32*1024)
	for {
		readStart := time.Now()
		n, readErr := resp.Body.Read(buf)
		res.upstreamTime += time.Since(readStart)

		if n > 0 {
			idle.Reset(g.cfg.Timeouts.StreamIdle)
			res.keep(buf[:n], g.cfg.Capture.MaxBodyBytes)

			_, writeErr := w.Write(buf[:n])
			if writeErr == nil {
				// Without Flush the bytes would wait in a buffer until it
				// filled, and the client would see the answer in bursts.
				writeErr = flusher.Flush()
			}
			if writeErr != nil {
				// The client has gone. The deferred cancel stops the
				// upstream from generating tokens nobody will read.
				res.outcome = capturepb.Outcome_OUTCOME_CLIENT_CANCELLED
				return res
			}
		}

		if readErr == io.EOF {
			res.outcome = capturepb.Outcome_OUTCOME_COMPLETED
			return res
		}
		if readErr != nil {
			res.outcome = classify(r, readErr, idleFired.Load())
			// Part of the stream has been sent and cannot be taken back.
			// Cut the connection so the client can tell it is incomplete.
			res.abort = res.outcome != capturepb.Outcome_OUTCOME_CLIENT_CANCELLED
			return res
		}
	}
}

// passThrough sends on a plain response that is longer than the buffer limit.
// held is what has been read so far, limit+1 bytes. It is written to the
// client, and the rest of the body is copied through as it arrives without
// being kept. The capture record gets the first limit bytes (or fewer, if the
// capture limit is lower) and is marked truncated.
func (g *Gateway) passThrough(w http.ResponseWriter, r *http.Request, resp *http.Response, held []byte, limit int, res *result, idleFired *atomic.Bool) {
	res.keep(held[:limit], g.cfg.Capture.MaxBodyBytes)
	res.bodyBytes += len(held) - limit
	res.truncated = true

	copyHeaders(w.Header(), resp.Header)
	if resp.ContentLength >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
	}
	w.WriteHeader(resp.StatusCode)
	res.status = resp.StatusCode

	// Flush after every write. Otherwise net/http holds the bytes in its
	// write buffer, and a slow upstream would leave the client waiting with
	// nothing.
	flusher := http.NewResponseController(w)
	send := func(data []byte) bool {
		_, err := w.Write(data)
		if err == nil {
			err = flusher.Flush()
		}
		if err != nil {
			res.outcome = capturepb.Outcome_OUTCOME_CLIENT_CANCELLED
			return false
		}
		return true
	}

	if !send(held) {
		return
	}
	buf := make([]byte, 32*1024)
	for {
		readStart := time.Now()
		n, readErr := resp.Body.Read(buf)
		res.upstreamTime += time.Since(readStart)

		if n > 0 {
			res.bodyBytes += n
			if !send(buf[:n]) {
				return
			}
		}
		if readErr == io.EOF {
			res.outcome = capturepb.Outcome_OUTCOME_COMPLETED
			return
		}
		if readErr != nil {
			res.outcome = classify(r, readErr, idleFired.Load())
			// The status and part of the body have been sent. Cut the
			// connection so the client can tell the body is incomplete.
			res.abort = res.outcome != capturepb.Outcome_OUTCOME_CLIENT_CANCELLED
			return
		}
	}
}

// fail handles an upstream failure that happened before anything was sent to
// the client. Unless the client has gone, it gets a 502 or 504.
func (g *Gateway) fail(w http.ResponseWriter, r *http.Request, res *result, err error, idleFired bool) {
	res.outcome = classify(r, err, idleFired)

	var message string
	switch res.outcome {
	case capturepb.Outcome_OUTCOME_CLIENT_CANCELLED:
		return // nobody to answer; status stays 0
	case capturepb.Outcome_OUTCOME_UPSTREAM_TIMEOUT:
		res.status = http.StatusGatewayTimeout
		message = "upstream timed out"
	default:
		res.status = http.StatusBadGateway
		message = "upstream request failed"
	}
	// The client gets a fixed message. The underlying error can contain
	// internal addresses, so it goes to the log only.
	g.logger.Warn("upstream call failed", "request_id", w.Header().Get("X-Request-ID"), "error", err)

	data := errorBody("upstream_error", message)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(res.status)
	w.Write(data)
	res.keep(data, g.cfg.Capture.MaxBodyBytes)
}

// classify decides why an upstream call or read failed.
func classify(r *http.Request, err error, idleFired bool) capturepb.Outcome {
	// Check the client first: when it disconnects, the upstream call fails
	// with "context canceled", which would otherwise look like an upstream
	// fault.
	if r.Context().Err() != nil {
		return capturepb.Outcome_OUTCOME_CLIENT_CANCELLED
	}
	if idleFired || isTimeout(err) {
		return capturepb.Outcome_OUTCOME_UPSTREAM_TIMEOUT
	}
	return capturepb.Outcome_OUTCOME_UPSTREAM_ERROR
}

// shouldRetry reports whether an attempt failed in a way a second attempt
// might fix. Timeouts are not retried: the client has waited long enough.
func shouldRetry(resp *http.Response, err error) bool {
	if err != nil {
		return !isTimeout(err) && !errors.Is(err, context.Canceled)
	}
	switch resp.StatusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// sleep waits for d and returns true, or returns false early if ctx ends.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// keep appends data to the captured copy of the body, up to limit bytes.
func (res *result) keep(data []byte, limit int) {
	res.bodyBytes += len(data)
	room := limit - len(res.body)
	if len(data) > room {
		data = data[:room]
		res.truncated = true
	}
	res.body = append(res.body, data...)
}

// copyHeaders passes on the few upstream headers a client needs. Everything
// else, such as cookies and connection details, stays behind.
func copyHeaders(dst, src http.Header) {
	for name, values := range src {
		lower := strings.ToLower(name)
		wanted := lower == "content-type" || lower == "cache-control" || lower == "retry-after" ||
			strings.HasPrefix(lower, "x-ratelimit-")
		if wanted {
			dst[name] = values
		}
	}
}
