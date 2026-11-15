package proxy

import "testing"

func TestExtractUsage(t *testing.T) {
	plain := `{"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":5,"total_tokens":8}}`
	if u := extractUsage([]byte(plain), false); u.GetPromptTokens() != 3 || u.GetCompletionTokens() != 5 || u.GetTotalTokens() != 8 {
		t.Errorf("plain: %v", u)
	}

	stream := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}],\"usage\":null}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n" +
		"data: [DONE]\n\n"
	if u := extractUsage([]byte(stream), true); u.GetTotalTokens() != 3 {
		t.Errorf("stream: %v", u)
	}

	for name, body := range map[string]string{
		"stream without usage": "data: {\"choices\":[]}\n\ndata: [DONE]\n\n",
		"error body":           `{"error":{"message":"nope"}}`,
		"not JSON":             "<html>",
		"empty":                "",
	} {
		if u := extractUsage([]byte(body), name == "stream without usage"); u != nil {
			t.Errorf("%s: got %v, want nil", name, u)
		}
	}
}

func TestValidRequestID(t *testing.T) {
	for id, want := range map[string]bool{
		"abc-123_X.y":             true,
		"":                        false,
		"has space":               false,
		"new\nline":               false,
		string(make([]byte, 129)): false,
	} {
		if got := validRequestID(id); got != want {
			t.Errorf("validRequestID(%q) = %v, want %v", id, got, want)
		}
	}
}
