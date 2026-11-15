// Package fakeupstream is a stand-in for an OpenAI-compatible provider. It
// answers POST /v1/chat/completions with made-up text after a configurable
// delay, and fails a configurable share of requests. Tests and benchmarks use
// it so that nothing ever calls a real provider.
package fakeupstream

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Latency distributions.
const (
	DistFixed       = "fixed"       // always Latency
	DistUniform     = "uniform"     // evenly spread between Latency and LatencyMax
	DistExponential = "exponential" // random with mean Latency; mostly short, sometimes long
)

// Config controls the fake's behaviour.
type Config struct {
	// Seed for the random choices. The same seed gives the same sequence of
	// latencies and errors, in order of request arrival.
	Seed uint64

	// Delay before the response starts. See the Dist constants.
	LatencyDist string
	Latency     time.Duration
	LatencyMax  time.Duration // only used by DistUniform

	// Share of requests answered with HTTP 503, from 0 to 1.
	ErrorRate float64

	// Completion tokens per response. A stream sends one chunk per token.
	Tokens int
	// Pause between stream chunks.
	ChunkDelay time.Duration

	// If set, requests must carry "Authorization: Bearer <RequireKey>".
	RequireKey string
}

// Server is the fake upstream. Use Handler to serve it.
type Server struct {
	cfg Config

	mu  sync.Mutex // guards rng
	rng *rand.Rand

	requests  atomic.Int64
	cancelled atomic.Int64
}

// New creates a fake upstream.
func New(cfg Config) *Server {
	if cfg.LatencyDist == "" {
		cfg.LatencyDist = DistFixed
	}
	if cfg.Tokens <= 0 {
		cfg.Tokens = 16
	}
	return &Server{cfg: cfg, rng: rand.New(rand.NewPCG(cfg.Seed, 0))}
}

// Requests returns how many chat completion requests have arrived.
func (s *Server) Requests() int64 { return s.requests.Load() }

// Cancelled returns how many requests the caller abandoned before the
// response was complete.
func (s *Server) Cancelled() int64 { return s.cancelled.Load() }

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.chatCompletions)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	return mux
}

// plan draws this request's latency and whether it fails. Both draws happen
// under one lock, in arrival order, which is what makes a run repeatable.
func (s *Server) plan() (latency time.Duration, fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.cfg.LatencyDist {
	case DistUniform:
		span := s.cfg.LatencyMax - s.cfg.Latency
		latency = s.cfg.Latency
		if span > 0 {
			latency += time.Duration(s.rng.Float64() * float64(span))
		}
	case DistExponential:
		latency = time.Duration(s.rng.ExpFloat64() * float64(s.cfg.Latency))
	default:
		latency = s.cfg.Latency
	}
	fail = s.rng.Float64() < s.cfg.ErrorRate
	return latency, fail
}

type chatRequest struct {
	Model         string `json:"model"`
	Stream        bool   `json:"stream"`
	StreamOptions struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
	Messages []struct {
		Content any `json:"content"`
	} `json:"messages"`
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

func (s *Server) chatCompletions(w http.ResponseWriter, r *http.Request) {
	n := s.requests.Add(1)

	if s.cfg.RequireKey != "" && r.Header.Get("Authorization") != "Bearer "+s.cfg.RequireKey {
		writeError(w, http.StatusUnauthorized, "invalid_api_key", "incorrect API key")
		return
	}

	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "body is not valid JSON")
		return
	}

	latency, fail := s.plan()
	if !s.sleep(r, latency) {
		return
	}
	if fail {
		writeError(w, http.StatusServiceUnavailable, "server_error", "injected failure")
		return
	}

	id := fmt.Sprintf("chatcmpl-fake-%d", n)
	created := time.Now().Unix()
	use := usage{PromptTokens: countPromptTokens(req), CompletionTokens: s.cfg.Tokens}
	use.TotalTokens = use.PromptTokens + use.CompletionTokens

	if req.Stream {
		s.stream(w, r, req, id, created, use)
		return
	}

	words := make([]string, s.cfg.Tokens)
	for i := range words {
		words[i] = fmt.Sprintf("tok%d", i)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": created,
		"model":   req.Model,
		"choices": []map[string]any{{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": strings.Join(words, " ")},
			"finish_reason": "stop",
		}},
		"usage": use,
	})
}

// stream sends the response as server-sent events: one "data:" line per
// token, flushed as it is written, then "data: [DONE]".
func (s *Server) stream(w http.ResponseWriter, r *http.Request, req chatRequest, id string, created int64, use usage) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	flusher := http.NewResponseController(w)

	send := func(payload any) {
		data, _ := json.Marshal(payload)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}
	chunk := func(delta map[string]any, finish any) map[string]any {
		return map[string]any{
			"id":      id,
			"object":  "chat.completion.chunk",
			"created": created,
			"model":   req.Model,
			"choices": []map[string]any{{"index": 0, "delta": delta, "finish_reason": finish}},
		}
	}

	for i := 0; i < s.cfg.Tokens; i++ {
		if i > 0 && !s.sleep(r, s.cfg.ChunkDelay) {
			return
		}
		send(chunk(map[string]any{"content": fmt.Sprintf("tok%d ", i)}, nil))
	}
	send(chunk(map[string]any{}, "stop"))

	if req.StreamOptions.IncludeUsage {
		final := chunk(nil, nil)
		final["choices"] = []any{}
		final["usage"] = use
		send(final)
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}

// sleep waits for d. It returns false, and counts a cancellation, if the
// caller went away first.
func (s *Server) sleep(r *http.Request, d time.Duration) bool {
	if d <= 0 {
		return r.Context().Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-r.Context().Done():
		s.cancelled.Add(1)
		return false
	}
}

// countPromptTokens is a stand-in for a tokenizer: one token per word.
func countPromptTokens(req chatRequest) int {
	total := 0
	for _, m := range req.Messages {
		if text, ok := m.Content.(string); ok {
			total += len(strings.Fields(text))
		}
	}
	return total
}

func writeError(w http.ResponseWriter, status int, kind, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"message": message, "type": kind},
	})
}
