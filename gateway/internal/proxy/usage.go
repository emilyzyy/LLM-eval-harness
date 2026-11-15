package proxy

import (
	"bytes"
	"encoding/json"

	"github.com/emilyzyy/LLM-eval-harness/gateway/gen/go/capturepb"
)

type usagePayload struct {
	Usage *struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
		TotalTokens      int64 `json:"total_tokens"`
	} `json:"usage"`
}

// extractUsage pulls the token counts out of a response body. It returns nil
// if the body has none.
//
// A plain response is one JSON object with a "usage" field. A stream is a
// series of "data: {...}" lines, and only the last chunk carries usage, and
// only if the client asked for it with stream_options.include_usage.
func extractUsage(body []byte, sse bool) *capturepb.TokenUsage {
	if !sse {
		return parseUsage(body)
	}

	var found *capturepb.TokenUsage
	for _, line := range bytes.Split(body, []byte("\n")) {
		data, isData := bytes.CutPrefix(line, []byte("data:"))
		// Skip lines that cannot hold usage without parsing them.
		if !isData || !bytes.Contains(data, []byte(`"usage"`)) {
			continue
		}
		if usage := parseUsage(data); usage != nil {
			found = usage
		}
	}
	return found
}

func parseUsage(data []byte) *capturepb.TokenUsage {
	var payload usagePayload
	if err := json.Unmarshal(data, &payload); err != nil || payload.Usage == nil {
		return nil
	}
	return &capturepb.TokenUsage{
		PromptTokens:     payload.Usage.PromptTokens,
		CompletionTokens: payload.Usage.CompletionTokens,
		TotalTokens:      payload.Usage.TotalTokens,
	}
}
