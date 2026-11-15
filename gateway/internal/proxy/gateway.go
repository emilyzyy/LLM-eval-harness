// Package proxy is the gateway's HTTP server: it authenticates a client,
// forwards its chat completion request to an upstream, relays the response,
// and hands a capture record to the capture queue.
package proxy

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"golang.org/x/time/rate"

	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/capture"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/config"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/metrics"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/upstream"
)

// Gateway holds everything the request handlers share.
type Gateway struct {
	cfg     config.Config
	logger  *slog.Logger
	metrics *metrics.Metrics

	// byModel maps a model name to the route that serves it. fallback serves
	// every other model and may be nil.
	byModel  map[string]*route
	fallback *route

	// queue and sink are nil when capture is off.
	queue *capture.Queue
	sink  capture.Sink
}

// route is one upstream together with its rate limiter.
type route struct {
	upstream upstream.Upstream
	// limiter is nil when the upstream has no rate limit.
	limiter *rate.Limiter
}

// New builds a gateway from its configuration. sink receives the capture
// records; pass nil to turn capture off.
func New(cfg config.Config, sink capture.Sink, m *metrics.Metrics, logger *slog.Logger) *Gateway {
	g := &Gateway{
		cfg:     cfg,
		logger:  logger,
		metrics: m,
		byModel: map[string]*route{},
		sink:    sink,
	}

	for _, u := range cfg.Upstreams {
		rt := &route{
			upstream: upstream.NewOpenAI(u.Name, u.BaseURL, u.APIKey, cfg.Timeouts.ResponseHeader),
		}
		if u.RateLimit.RequestsPerSecond > 0 {
			rt.limiter = rate.NewLimiter(rate.Limit(u.RateLimit.RequestsPerSecond), u.RateLimit.Burst)
		}
		for _, model := range u.Models {
			g.byModel[model] = rt
		}
		if u.Name == cfg.DefaultUpstream {
			g.fallback = rt
		}
	}

	if sink != nil {
		g.queue = capture.NewQueue(sink, cfg.Capture.QueueSize, capture.Hooks{
			Enqueued:  m.CaptureEnqueued.Inc,
			Dropped:   m.CaptureDropped.Inc,
			SinkError: m.CaptureSinkError.Inc,
		}, logger)
	}
	return g
}

// Handler returns the HTTP handler for every endpoint the gateway serves.
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", g.handleChat)
	mux.HandleFunc("GET /healthz", g.handleHealthz)
	mux.HandleFunc("GET /readyz", g.handleReadyz)
	mux.Handle("GET /metrics", g.metrics.Handler())
	return mux
}

// Shutdown writes out pending capture records, waiting no longer than ctx
// allows. Call it after the HTTP server has stopped.
func (g *Gateway) Shutdown(ctx context.Context) error {
	if g.queue == nil {
		return nil
	}
	return g.queue.Shutdown(ctx)
}

// routeFor picks the upstream for a model. It returns nil if no upstream
// lists the model and there is no default.
func (g *Gateway) routeFor(model string) *route {
	if rt, ok := g.byModel[model]; ok {
		return rt
	}
	return g.fallback
}

// handleHealthz answers "is the process running?".
func (g *Gateway) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

// handleReadyz answers "can this process serve requests?". That needs an
// upstream and nothing else. Kafka's state is shown for operators but does
// not change the answer: the gateway serves clients with Kafka down.
func (g *Gateway) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	body := map[string]any{
		"status":    "ready",
		"upstreams": len(g.cfg.Upstreams),
		"capture":   g.cfg.Capture.Sink,
	}
	if reporter, ok := g.sink.(capture.StateReporter); ok {
		body["kafka"] = reporter.State()
	}

	status := http.StatusOK
	if len(g.cfg.Upstreams) == 0 {
		body["status"] = "no upstream configured"
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

// errorBody builds an error in the shape OpenAI clients expect.
func errorBody(kind, message string) []byte {
	data, _ := json.Marshal(map[string]any{
		"error": map[string]any{"message": message, "type": kind},
	})
	return append(data, '\n')
}
