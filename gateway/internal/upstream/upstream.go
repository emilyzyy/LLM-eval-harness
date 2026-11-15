// Package upstream is the gateway's view of a model provider.
package upstream

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"strings"
	"time"
)

// Request is one chat completion call.
type Request struct {
	// The JSON body, forwarded unchanged.
	Body []byte
	// Sent to the upstream as X-Request-ID so both sides log the same ID.
	RequestID string
	// True if the body asks for a streamed response.
	Stream bool
}

// Upstream sends chat completion requests to one provider.
//
// ChatCompletions returns as soon as the provider has sent its response
// headers. The caller reads the body, which for a stream arrives over time,
// and must close it. Cancelling ctx aborts the call, including a body that is
// still being read.
type Upstream interface {
	Name() string
	ChatCompletions(ctx context.Context, req Request) (*http.Response, error)
}

// OpenAI talks to any server that implements OpenAI's
// POST /v1/chat/completions.
type OpenAI struct {
	name   string
	url    string
	apiKey string
	client *http.Client
}

// NewOpenAI builds an upstream. baseURL is the server address without the
// /v1/chat/completions path. responseHeaderTimeout limits how long the server
// may take to start responding; the caller's context limits everything else.
func NewOpenAI(name, baseURL, apiKey string, responseHeaderTimeout time.Duration) *OpenAI {
	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: responseHeaderTimeout,
		// Keep enough idle connections that a busy gateway reuses them
		// instead of opening a new one per request. The default of 2 per
		// host is far too low for a proxy with a single destination.
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 512,
		IdleConnTimeout:     90 * time.Second,
		// Ask for uncompressed responses, so stream chunks can be relayed
		// byte for byte.
		DisableCompression: true,
	}
	return &OpenAI{
		name:   name,
		url:    strings.TrimRight(baseURL, "/") + "/v1/chat/completions",
		apiKey: apiKey,
		// No Client.Timeout: it would also cut off long streams. Deadlines
		// come from the context.
		client: &http.Client{Transport: transport},
	}
}

func (o *OpenAI) Name() string { return o.name }

func (o *OpenAI) ChatCompletions(ctx context.Context, req Request) (*http.Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, o.url, bytes.NewReader(req.Body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if req.Stream {
		httpReq.Header.Set("Accept", "text/event-stream")
	} else {
		httpReq.Header.Set("Accept", "application/json")
	}
	if o.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+o.apiKey)
	}
	if req.RequestID != "" {
		httpReq.Header.Set("X-Request-ID", req.RequestID)
	}
	return o.client.Do(httpReq)
}
