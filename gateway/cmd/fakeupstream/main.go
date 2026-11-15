// Command fakeupstream runs a pretend OpenAI-compatible provider for local
// development, tests and benchmarks.
//
//	fakeupstream -addr :9090 -latency 50ms -latency-dist exponential -error-rate 0.01
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/fakeupstream"
)

func main() {
	var cfg fakeupstream.Config
	addr := flag.String("addr", ":9090", "address to listen on")
	flag.Uint64Var(&cfg.Seed, "seed", 1, "seed for random latency and errors")
	flag.StringVar(&cfg.LatencyDist, "latency-dist", fakeupstream.DistFixed, "latency distribution: fixed, uniform or exponential")
	flag.DurationVar(&cfg.Latency, "latency", 20*time.Millisecond, "fixed latency, exponential mean, or uniform lower bound")
	flag.DurationVar(&cfg.LatencyMax, "latency-max", 0, "uniform upper bound")
	flag.Float64Var(&cfg.ErrorRate, "error-rate", 0, "share of requests answered with 503, from 0 to 1")
	flag.IntVar(&cfg.Tokens, "tokens", 16, "completion tokens per response")
	flag.DurationVar(&cfg.ChunkDelay, "chunk-delay", 10*time.Millisecond, "pause between stream chunks")
	flag.StringVar(&cfg.RequireKey, "require-key", os.Getenv("FAKE_UPSTREAM_KEY"), "if set, the API key callers must send (default: $FAKE_UPSTREAM_KEY)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	switch cfg.LatencyDist {
	case fakeupstream.DistFixed, fakeupstream.DistUniform, fakeupstream.DistExponential:
	default:
		logger.Error("unknown -latency-dist", "value", cfg.LatencyDist)
		os.Exit(2)
	}

	server := &http.Server{
		Addr:              *addr,
		Handler:           fakeupstream.New(cfg).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	logger.Info("fake upstream listening", "addr", *addr, "latency", cfg.Latency.String(),
		"latency_dist", cfg.LatencyDist, "error_rate", cfg.ErrorRate, "tokens", cfg.Tokens, "seed", cfg.Seed)
	if err := server.ListenAndServe(); err != nil {
		logger.Error("fake upstream stopped", "error", err)
		os.Exit(1)
	}
}
