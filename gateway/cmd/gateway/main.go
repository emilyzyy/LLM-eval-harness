// Command gateway runs the proxy server.
//
//	gateway -config config.yaml
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/capture"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/config"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/metrics"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/proxy"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to the YAML config file")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(*configPath, logger); err != nil {
		logger.Error("gateway stopped with an error", "error", err)
		os.Exit(1)
	}
}

func run(configPath string, logger *slog.Logger) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	m := metrics.New()
	sink, err := newSink(cfg.Capture, m, logger)
	if err != nil {
		return err
	}
	gateway := proxy.New(cfg, sink, m, logger)

	server := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: gateway.Handler(),
		// Limit how long a client may take to send its headers and how long
		// an unused keep-alive connection stays open. There is deliberately
		// no WriteTimeout: it would cut off long streams.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// ctx is cancelled on Ctrl-C or SIGTERM, which is what Docker and
	// Kubernetes send to stop a container.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	logger.Info("gateway listening", "addr", cfg.ListenAddr, "capture_sink", cfg.Capture.Sink, "upstreams", len(cfg.Upstreams))

	select {
	case err := <-serverErr:
		return err // could not listen
	case <-ctx.Done():
	}
	logger.Info("shutting down")

	// Step 1: stop accepting requests and let those in flight finish.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Warn("some requests did not finish before the shutdown deadline", "error", err)
	}

	// Step 2: write out the capture records those requests produced.
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), cfg.Capture.FlushTimeout)
	defer cancelFlush()
	if err := gateway.Shutdown(flushCtx); err != nil {
		logger.Warn("capture records may have been lost at shutdown", "error", err)
	}
	logger.Info("stopped")
	return nil
}

// newSink builds the capture sink the config asks for. It returns nil when
// capture is off.
func newSink(cfg config.Capture, m *metrics.Metrics, logger *slog.Logger) (capture.Sink, error) {
	switch cfg.Sink {
	case "jsonl":
		return capture.NewJSONLSink(cfg.JSONL.Path)
	case "kafka":
		return capture.NewKafkaSink(capture.KafkaConfig{
			Brokers:            cfg.Kafka.Brokers,
			Topic:              cfg.Kafka.Topic,
			Partitions:         cfg.Kafka.Partitions,
			ReplicationFactor:  cfg.Kafka.ReplicationFactor,
			DeliveryTimeout:    cfg.Kafka.DeliveryTimeout,
			MaxBufferedRecords: cfg.Kafka.MaxBufferedRecords,
			OnProduceError:     m.KafkaProduceErr.Inc,
		}, logger)
	default:
		return nil, nil
	}
}
