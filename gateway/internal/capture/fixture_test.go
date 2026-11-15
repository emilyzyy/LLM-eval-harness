package capture

import (
	"flag"
	"os"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/emilyzyy/LLM-eval-harness/gateway/gen/go/capturepb"
)

// The fixture is one encoded capture record, checked in. This test proves the
// Go code still reads it; schema_test/test_capture_schema.py proves the
// Python code reads the same bytes.
//
// After changing the schema or the record below, regenerate it with:
//
//	go test ./internal/capture -run TestFixture -update
const fixturePath = "../../schema_test/testdata/capture_record.bin"

var update = flag.Bool("update", false, "rewrite the schema test fixture")

func fixtureRecord() *capturepb.CaptureRecord {
	received := time.Date(2026, 1, 2, 3, 4, 5, 6000000, time.UTC)
	return &capturepb.CaptureRecord{
		RequestId:         "0195c2a0-7b1e-7c3a-9f00-0123456789ab",
		ReceivedAt:        timestamppb.New(received),
		CompletedAt:       timestamppb.New(received.Add(1250 * time.Millisecond)),
		Upstream:          "fake",
		Model:             "fake-model",
		Stream:            true,
		RequestBody:       []byte(`{"model":"fake-model","stream":true,"messages":[{"role":"user","content":"héllo"}]}`),
		ResponseBody:      []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"),
		ResponseTruncated: true,
		StatusCode:        200,
		Outcome:           capturepb.Outcome_OUTCOME_COMPLETED,
		Usage:             &capturepb.TokenUsage{PromptTokens: 11, CompletionTokens: 22, TotalTokens: 33},
		UpstreamLatency:   durationpb.New(1200 * time.Millisecond),
		TotalLatency:      durationpb.New(1250 * time.Millisecond),
		Attempts:          2,
	}
}

func TestFixture(t *testing.T) {
	want := fixtureRecord()
	if *update {
		data, err := proto.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fixturePath, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	data, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("%v (run with -update to create it)", err)
	}
	var got capturepb.CaptureRecord
	if err := proto.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&got, want) {
		t.Errorf("fixture does not match fixtureRecord(); run with -update\n got: %v\nwant: %v", &got, want)
	}
}
