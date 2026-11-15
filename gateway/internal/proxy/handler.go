package proxy

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/emilyzyy/LLM-eval-harness/gateway/gen/go/capturepb"
)

// statusClientClosed is logged and counted when the client disconnects before
// the gateway has sent a status. It is never written to the wire. (499 is the
// code nginx uses for the same situation.)
const statusClientClosed = 499

// handleChat serves POST /v1/chat/completions.
func (g *Gateway) handleChat(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	g.metrics.InFlight.Inc()
	defer g.metrics.InFlight.Dec()

	requestID := requestIDFor(r)
	w.Header().Set("X-Request-ID", requestID)
	logger := g.logger.With("request_id", requestID)

	// reject answers a request that never reaches an upstream.
	reject := func(status int, kind, message string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		w.Write(errorBody(kind, message))
		g.metrics.ObserveRequest(status, time.Since(start))
		logger.Info("request rejected", "status", status, "reason", message)
	}

	if !g.authorized(r) {
		reject(http.StatusUnauthorized, "invalid_api_key", "missing or incorrect proxy key")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, g.cfg.MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			reject(http.StatusRequestEntityTooLarge, "invalid_request_error", "request body is too large")
		} else {
			reject(http.StatusBadRequest, "invalid_request_error", "could not read request body")
		}
		return
	}

	// The gateway needs only two fields. The body is forwarded as received.
	var fields struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(body, &fields); err != nil {
		reject(http.StatusBadRequest, "invalid_request_error", "request body is not valid JSON")
		return
	}

	rt := g.routeFor(fields.Model)
	if rt == nil {
		reject(http.StatusNotFound, "invalid_request_error", "no upstream serves model "+strconv.Quote(fields.Model))
		return
	}

	// Allow takes a token if one is available and never waits. Making the
	// client wait instead would hide the limit and pile up connections.
	if rt.limiter != nil && !rt.limiter.Allow() {
		g.metrics.RateLimited.WithLabelValues(rt.upstream.Name()).Inc()
		w.Header().Set("Retry-After", retryAfter(float64(rt.limiter.Limit())))
		reject(http.StatusTooManyRequests, "rate_limit_exceeded", "gateway rate limit exceeded for upstream "+rt.upstream.Name())
		return
	}

	res := g.forward(w, r, rt, body, fields.Stream, requestID)
	total := time.Since(start)

	// The client already has its response. Everything below is bookkeeping.
	metricStatus := res.status
	if metricStatus == 0 {
		metricStatus = statusClientClosed
	}
	g.metrics.ObserveRequest(metricStatus, total)
	g.metrics.ObserveUpstream(total, res.upstreamTime)

	logger.Info("request completed",
		"upstream", rt.upstream.Name(),
		"model", fields.Model,
		"stream", fields.Stream,
		"status", metricStatus,
		"outcome", res.outcome.String(),
		"attempts", res.attempts,
		"upstream_ms", milliseconds(res.upstreamTime),
		"total_ms", milliseconds(total),
		"response_bytes", res.bodyBytes,
	)

	if g.queue != nil {
		limit := g.cfg.Capture.MaxBodyBytes
		record := &capturepb.CaptureRecord{
			RequestId:         requestID,
			ReceivedAt:        timestamppb.New(start),
			CompletedAt:       timestamppb.New(start.Add(total)),
			Upstream:          rt.upstream.Name(),
			Model:             fields.Model,
			Stream:            fields.Stream,
			RequestBody:       body[:min(len(body), limit)],
			RequestTruncated:  len(body) > limit,
			ResponseBody:      res.body,
			ResponseTruncated: res.truncated,
			StatusCode:        int32(res.status),
			Outcome:           res.outcome,
			UpstreamLatency:   durationpb.New(res.upstreamTime),
			TotalLatency:      durationpb.New(total),
			Attempts:          int32(res.attempts),
		}
		if !res.truncated {
			record.Usage = extractUsage(res.body, res.sse)
		}
		g.queue.Enqueue(record)
	}

	if res.abort {
		// The stream broke off part-way. Returning normally would end the
		// response cleanly and the client could mistake it for a complete
		// answer. This panic is the standard way to tell net/http to drop
		// the connection instead, so the client sees an error.
		panic(http.ErrAbortHandler)
	}
}

// authorized checks the client's "Authorization: Bearer <proxy key>" header.
func (g *Gateway) authorized(r *http.Request) bool {
	key, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !found {
		return false
	}
	// A constant-time comparison does not reveal, through how long it takes,
	// how many leading characters of a guess were right.
	return subtle.ConstantTimeCompare([]byte(key), []byte(g.cfg.ProxyKey)) == 1
}

// requestIDFor returns the client's X-Request-ID if it is safe to echo into
// headers and logs, and otherwise a new UUIDv7. A UUIDv7 starts with a
// timestamp, so IDs sort by creation time.
func requestIDFor(r *http.Request) string {
	if id := r.Header.Get("X-Request-ID"); validRequestID(id) {
		return id
	}
	return uuid.Must(uuid.NewV7()).String()
}

func validRequestID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		isLetter := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		isDigit := c >= '0' && c <= '9'
		if !isLetter && !isDigit && c != '-' && c != '_' && c != '.' {
			return false
		}
	}
	return true
}

// retryAfter is how many whole seconds until the bucket has a token again.
func retryAfter(tokensPerSecond float64) string {
	seconds := int(math.Ceil(1 / tokensPerSecond))
	return strconv.Itoa(max(seconds, 1))
}

func milliseconds(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}
