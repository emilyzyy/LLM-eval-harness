// Package config loads the gateway's YAML configuration.
//
// Secrets are never written in the file. The file names the environment
// variables that hold them, and Load reads those variables.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"go.yaml.in/yaml/v3"
)

// Config is the whole configuration file.
type Config struct {
	// Address the server listens on, for example ":8080".
	ListenAddr string `yaml:"listen_addr"`

	// Name of the environment variable holding the key clients must present.
	ProxyKeyEnv string `yaml:"proxy_key_env"`

	// Largest request body accepted, in bytes.
	MaxBodyBytes int64 `yaml:"max_body_bytes"`

	// Largest non-streaming response held in memory before it is sent on, in
	// bytes. A longer response is passed through to the client as it arrives,
	// and its capture record keeps only the part that was held, marked
	// truncated.
	MaxResponseBufferBytes int64 `yaml:"max_response_buffer_bytes"`

	Upstreams []Upstream `yaml:"upstreams"`

	// Name of the upstream used when no upstream lists the requested model.
	// Empty means such requests are rejected.
	DefaultUpstream string `yaml:"default_upstream"`

	Timeouts Timeouts `yaml:"timeouts"`
	Retry    Retry    `yaml:"retry"`
	Capture  Capture  `yaml:"capture"`

	// How long shutdown waits for in-flight requests to finish.
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`

	// ProxyKey is read from the ProxyKeyEnv variable by Load.
	ProxyKey string `yaml:"-"`
}

// Upstream is one provider endpoint.
type Upstream struct {
	Name string `yaml:"name"`
	// Base URL without the /v1/chat/completions path.
	BaseURL string `yaml:"base_url"`
	// Name of the environment variable holding this upstream's API key.
	APIKeyEnv string `yaml:"api_key_env"`
	// Models routed to this upstream.
	Models    []string  `yaml:"models"`
	RateLimit RateLimit `yaml:"rate_limit"`

	// APIKey is read from the APIKeyEnv variable by Load.
	APIKey string `yaml:"-"`
}

// RateLimit configures a token bucket. One request spends one token.
type RateLimit struct {
	// Tokens added per second. Zero disables rate limiting.
	RequestsPerSecond float64 `yaml:"requests_per_second"`
	// Bucket size: how many requests may pass at once after an idle period.
	Burst int `yaml:"burst"`
}

// Timeouts bound how long the gateway waits on an upstream.
type Timeouts struct {
	// Non-streaming: limit for the whole request, retries included.
	Request time.Duration `yaml:"request"`
	// Limit for the upstream to start responding (send its headers).
	ResponseHeader time.Duration `yaml:"response_header"`
	// Streaming: longest allowed gap between two chunks.
	StreamIdle time.Duration `yaml:"stream_idle"`
}

// Retry controls retries of upstream calls. A call is only retried while
// nothing has been sent to the client.
type Retry struct {
	// Total attempts per request. 1 means never retry.
	MaxAttempts int `yaml:"max_attempts"`
	// Pause before each retry.
	Backoff time.Duration `yaml:"backoff"`
}

// Capture configures where capture records go.
type Capture struct {
	// "kafka", "jsonl" or "none".
	Sink string `yaml:"sink"`
	// Records that may wait for the background writer. When full, new
	// records are dropped.
	QueueSize int `yaml:"queue_size"`
	// Largest request or response body stored in a record, in bytes. Longer
	// bodies are cut and marked truncated.
	MaxBodyBytes int `yaml:"max_body_bytes"`
	// How long shutdown waits for pending records to be written.
	FlushTimeout time.Duration `yaml:"flush_timeout"`

	JSONL JSONL `yaml:"jsonl"`
	Kafka Kafka `yaml:"kafka"`
}

// JSONL configures the file sink.
type JSONL struct {
	Path string `yaml:"path"`
}

// Kafka configures the Kafka sink.
type Kafka struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
	// Partition count used if the gateway creates the topic. An existing
	// topic is left as it is.
	Partitions int32 `yaml:"partitions"`
	// Replication factor used if the gateway creates the topic.
	ReplicationFactor int16 `yaml:"replication_factor"`
	// A record not acknowledged within this time is given up on.
	DeliveryTimeout time.Duration `yaml:"delivery_timeout"`
	// Records the producer may hold in memory while waiting on the broker.
	// When full, new records fail immediately.
	MaxBufferedRecords int `yaml:"max_buffered_records"`
}

// Default returns the values used for anything the file leaves out.
func Default() Config {
	return Config{
		ListenAddr:   ":8080",
		ProxyKeyEnv:  "GATEWAY_PROXY_KEY",
		MaxBodyBytes: 4 << 20,
		// Far above a normal chat completion, and bounds the memory each
		// in-flight request can take.
		MaxResponseBufferBytes: 4 << 20,
		Timeouts: Timeouts{
			Request:        60 * time.Second,
			ResponseHeader: 30 * time.Second,
			StreamIdle:     30 * time.Second,
		},
		Retry: Retry{MaxAttempts: 2, Backoff: 100 * time.Millisecond},
		Capture: Capture{
			Sink:      "none",
			QueueSize: 1024,
			// Two bodies of this size plus the other fields stay under
			// Kafka's default 1 MB message limit.
			MaxBodyBytes: 256 << 10,
			FlushTimeout: 5 * time.Second,
			JSONL:        JSONL{Path: "captures.jsonl"},
			Kafka: Kafka{
				Topic:              "captures",
				Partitions:         3,
				ReplicationFactor:  1,
				DeliveryTimeout:    10 * time.Second,
				MaxBufferedRecords: 10000,
			},
		},
		ShutdownTimeout: 15 * time.Second,
	}
}

// Load reads the file at path, fills in defaults, reads the secrets from the
// environment and validates the result.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	return Parse(data, os.Getenv)
}

// Parse is Load without the file system: it takes the YAML text and a function
// that looks up environment variables.
func Parse(data []byte, getenv func(string) string) (Config, error) {
	cfg := Default()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	// A misspelled key is an error rather than a silently ignored setting.
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	cfg.ProxyKey = getenv(cfg.ProxyKeyEnv)
	for i := range cfg.Upstreams {
		cfg.Upstreams[i].APIKey = getenv(cfg.Upstreams[i].APIKeyEnv)
	}

	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

func (c Config) validate() error {
	if c.ProxyKeyEnv == "" {
		return errors.New("proxy_key_env is required")
	}
	if c.ProxyKey == "" {
		return fmt.Errorf("environment variable %s (proxy key) is not set", c.ProxyKeyEnv)
	}
	if c.MaxBodyBytes <= 0 {
		return errors.New("max_body_bytes must be positive")
	}
	if c.MaxResponseBufferBytes <= 0 {
		return errors.New("max_response_buffer_bytes must be positive")
	}
	if len(c.Upstreams) == 0 {
		return errors.New("at least one upstream is required")
	}

	names := map[string]bool{}
	models := map[string]string{}
	for _, u := range c.Upstreams {
		if u.Name == "" {
			return errors.New("every upstream needs a name")
		}
		if names[u.Name] {
			return fmt.Errorf("upstream %q is defined twice", u.Name)
		}
		names[u.Name] = true

		parsed, err := url.Parse(u.BaseURL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("upstream %q: base_url must be an http or https URL", u.Name)
		}
		// An upstream with no api_key_env is called without an Authorization
		// header. One that names a variable must find it set.
		if u.APIKeyEnv != "" && u.APIKey == "" {
			return fmt.Errorf("upstream %q: environment variable %s is not set", u.Name, u.APIKeyEnv)
		}
		if u.RateLimit.RequestsPerSecond < 0 {
			return fmt.Errorf("upstream %q: requests_per_second must not be negative", u.Name)
		}
		if u.RateLimit.RequestsPerSecond > 0 && u.RateLimit.Burst < 1 {
			return fmt.Errorf("upstream %q: burst must be at least 1 when rate limiting is on", u.Name)
		}
		for _, m := range u.Models {
			if other, taken := models[m]; taken {
				return fmt.Errorf("model %q is listed by both %q and %q", m, other, u.Name)
			}
			models[m] = u.Name
		}
	}
	if c.DefaultUpstream != "" && !names[c.DefaultUpstream] {
		return fmt.Errorf("default_upstream %q is not a defined upstream", c.DefaultUpstream)
	}

	if c.Timeouts.Request <= 0 || c.Timeouts.ResponseHeader <= 0 || c.Timeouts.StreamIdle <= 0 {
		return errors.New("timeouts must be positive")
	}
	if c.Retry.MaxAttempts < 1 {
		return errors.New("retry.max_attempts must be at least 1")
	}
	if c.Retry.Backoff < 0 {
		return errors.New("retry.backoff must not be negative")
	}
	if c.ShutdownTimeout <= 0 {
		return errors.New("shutdown_timeout must be positive")
	}

	switch c.Capture.Sink {
	case "none":
	case "jsonl":
		if c.Capture.JSONL.Path == "" {
			return errors.New("capture.jsonl.path is required for the jsonl sink")
		}
	case "kafka":
		k := c.Capture.Kafka
		if len(k.Brokers) == 0 {
			return errors.New("capture.kafka.brokers is required for the kafka sink")
		}
		if k.Topic == "" {
			return errors.New("capture.kafka.topic is required for the kafka sink")
		}
		if k.Partitions < 1 || k.ReplicationFactor < 1 {
			return errors.New("capture.kafka partitions and replication_factor must be at least 1")
		}
		if k.DeliveryTimeout <= 0 || k.MaxBufferedRecords < 1 {
			return errors.New("capture.kafka delivery_timeout and max_buffered_records must be positive")
		}
	default:
		return fmt.Errorf("capture.sink must be kafka, jsonl or none, got %q", c.Capture.Sink)
	}
	if c.Capture.QueueSize < 1 {
		return errors.New("capture.queue_size must be at least 1")
	}
	if c.Capture.MaxBodyBytes < 1 {
		return errors.New("capture.max_body_bytes must be at least 1")
	}
	if c.Capture.FlushTimeout <= 0 {
		return errors.New("capture.flush_timeout must be positive")
	}
	return nil
}
