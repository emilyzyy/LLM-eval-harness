package config

import (
	"strings"
	"testing"
	"time"
)

func env(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

const validYAML = `
listen_addr: ":9000"
proxy_key_env: MY_PROXY_KEY
upstreams:
  - name: fake
    base_url: http://localhost:9090
    api_key_env: FAKE_KEY
    models: [fake-model]
    rate_limit:
      requests_per_second: 50
      burst: 10
default_upstream: fake
timeouts:
  request: 45s
capture:
  sink: kafka
  kafka:
    brokers: [localhost:9092]
`

func TestParseAppliesFileEnvAndDefaults(t *testing.T) {
	cfg, err := Parse([]byte(validYAML), env(map[string]string{"MY_PROXY_KEY": "p", "FAKE_KEY": "u"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != ":9000" || cfg.ProxyKey != "p" || cfg.Upstreams[0].APIKey != "u" {
		t.Errorf("file or env values not applied: %+v", cfg)
	}
	if cfg.Timeouts.Request != 45*time.Second {
		t.Errorf("timeouts.request = %v", cfg.Timeouts.Request)
	}
	// Values the file leaves out keep their defaults.
	if cfg.Timeouts.StreamIdle != 30*time.Second || cfg.Capture.Kafka.Topic != "captures" || cfg.Capture.Kafka.Partitions != 3 {
		t.Errorf("defaults lost: %+v", cfg)
	}
	if cfg.Upstreams[0].RateLimit.Burst != 10 {
		t.Errorf("rate limit = %+v", cfg.Upstreams[0].RateLimit)
	}
}

func TestParseRejectsBadConfig(t *testing.T) {
	good := map[string]string{"MY_PROXY_KEY": "p", "FAKE_KEY": "u"}
	cases := []struct {
		name    string
		yaml    string
		env     map[string]string
		wantErr string
	}{
		{"proxy key not set", validYAML, map[string]string{"FAKE_KEY": "u"}, "MY_PROXY_KEY"},
		{"upstream key not set", validYAML, map[string]string{"MY_PROXY_KEY": "p"}, "FAKE_KEY"},
		{"misspelled key", strings.Replace(validYAML, "listen_addr", "listen_adr", 1), good, "listen_adr"},
		{"no upstreams", "proxy_key_env: MY_PROXY_KEY\n", good, "at least one upstream"},
		{"bad base_url", strings.Replace(validYAML, "http://localhost:9090", "localhost", 1), good, "base_url"},
		{"unknown default", strings.Replace(validYAML, "default_upstream: fake", "default_upstream: nope", 1), good, "nope"},
		{"unknown sink", strings.Replace(validYAML, "sink: kafka", "sink: redis", 1), good, "capture.sink"},
		{"kafka without brokers", strings.Replace(validYAML, "brokers: [localhost:9092]", "brokers: []", 1), good, "brokers"},
		{"rate limit without burst", strings.Replace(validYAML, "burst: 10", "burst: 0", 1), good, "burst"},
		{"zero response buffer", validYAML + "max_response_buffer_bytes: 0\n", good, "max_response_buffer_bytes"},
	}
	for _, c := range cases {
		_, err := Parse([]byte(c.yaml), env(c.env))
		if err == nil || !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("%s: err = %v, want one mentioning %q", c.name, err, c.wantErr)
		}
	}
}
