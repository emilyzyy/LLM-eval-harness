package fakeupstream

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func post(t *testing.T, url, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data)
}

// outcomes sends n requests one after another and records which ones failed.
func outcomes(t *testing.T, cfg Config, n int) string {
	server := httptest.NewServer(New(cfg).Handler())
	defer server.Close()
	var pattern strings.Builder
	for range n {
		status, _ := post(t, server.URL, `{"model":"m"}`)
		if status == http.StatusOK {
			pattern.WriteByte('.')
		} else {
			pattern.WriteByte('x')
		}
	}
	return pattern.String()
}

func TestSameSeedGivesSameErrors(t *testing.T) {
	cfg := Config{Seed: 7, ErrorRate: 0.3}
	first := outcomes(t, cfg, 60)
	second := outcomes(t, cfg, 60)
	if first != second {
		t.Errorf("same seed, different runs:\n%s\n%s", first, second)
	}

	failures := strings.Count(first, "x")
	if failures < 8 || failures > 30 {
		t.Errorf("%d of 60 requests failed at error rate 0.3", failures)
	}
	cfg.Seed = 8
	if other := outcomes(t, cfg, 60); other == first {
		t.Errorf("a different seed gave the same pattern")
	}
}

func TestSameSeedGivesSameLatencies(t *testing.T) {
	for _, dist := range []string{DistUniform, DistExponential} {
		cfg := Config{Seed: 3, LatencyDist: dist, Latency: 10 * time.Millisecond, LatencyMax: 30 * time.Millisecond}
		a, b := New(cfg), New(cfg)
		for i := range 100 {
			la, _ := a.plan()
			lb, _ := b.plan()
			if la != lb {
				t.Fatalf("%s: draw %d differs: %v vs %v", dist, i, la, lb)
			}
			if dist == DistUniform && (la < 10*time.Millisecond || la > 30*time.Millisecond) {
				t.Fatalf("uniform latency %v is outside 10ms-30ms", la)
			}
		}
	}
}

func TestStreamFormat(t *testing.T) {
	server := httptest.NewServer(New(Config{Tokens: 3}).Handler())
	defer server.Close()

	_, body := post(t, server.URL, `{"model":"m","stream":true,"stream_options":{"include_usage":true}}`)

	events := strings.Split(strings.TrimSpace(body), "\n\n")
	// 3 tokens, the finish chunk, the usage chunk, then [DONE].
	if len(events) != 6 {
		t.Fatalf("got %d events, want 6:\n%s", len(events), body)
	}
	if !strings.Contains(events[0], `"content":"tok0 "`) {
		t.Errorf("first event = %s", events[0])
	}
	if !strings.Contains(events[4], `"completion_tokens":3`) {
		t.Errorf("usage event = %s", events[4])
	}
	if events[5] != "data: [DONE]" {
		t.Errorf("last event = %s", events[5])
	}
}

func TestRequireKey(t *testing.T) {
	server := httptest.NewServer(New(Config{RequireKey: "secret"}).Handler())
	defer server.Close()
	if status, _ := post(t, server.URL, `{"model":"m"}`); status != http.StatusUnauthorized {
		t.Errorf("status without key = %d, want 401", status)
	}
}
