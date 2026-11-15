package capture

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/emilyzyy/LLM-eval-harness/gateway/gen/go/capturepb"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func record(i int) *capturepb.CaptureRecord {
	return &capturepb.CaptureRecord{RequestId: fmt.Sprintf("req-%d", i)}
}

func TestQueueDeliversInOrder(t *testing.T) {
	sink := &MemorySink{}
	q := NewQueue(sink, 10, Hooks{}, quiet)
	for i := range 5 {
		if !q.Enqueue(record(i)) {
			t.Fatalf("record %d was dropped from a queue with room", i)
		}
	}
	if err := q.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := sink.Records()
	if len(got) != 5 {
		t.Fatalf("sink has %d records, want 5", len(got))
	}
	for i, rec := range got {
		if rec.RequestId != fmt.Sprintf("req-%d", i) {
			t.Errorf("record %d is %s", i, rec.RequestId)
		}
	}
}

func TestQueueDropsWhenFullWithoutBlocking(t *testing.T) {
	sink := &MemorySink{Delay: 100 * time.Millisecond}
	dropped, enqueued := 0, 0
	q := NewQueue(sink, 2, Hooks{
		Enqueued: func() { enqueued++ },
		Dropped:  func() { dropped++ },
	}, quiet)

	start := time.Now()
	for i := range 50 {
		q.Enqueue(record(i))
	}
	if took := time.Since(start); took > 50*time.Millisecond {
		t.Errorf("50 Enqueue calls took %v; Enqueue must not wait for the sink", took)
	}
	// The queue holds 2 and the writer may have taken 1 or 2 more.
	if enqueued < 2 || enqueued > 4 || enqueued+dropped != 50 {
		t.Errorf("enqueued = %d, dropped = %d", enqueued, dropped)
	}
	q.Shutdown(context.Background())
}

func TestShutdownFlushesPendingRecords(t *testing.T) {
	sink := &MemorySink{Delay: 10 * time.Millisecond}
	q := NewQueue(sink, 100, Hooks{}, quiet)
	for i := range 20 {
		q.Enqueue(record(i))
	}
	// Nearly all 20 are still queued at this point.
	if err := q.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(sink.Records()); n != 20 {
		t.Errorf("sink has %d records after shutdown, want 20", n)
	}
}

func TestShutdownGivesUpAtDeadline(t *testing.T) {
	sink := &MemorySink{Delay: time.Second}
	q := NewQueue(sink, 100, Hooks{}, quiet)
	for i := range 10 {
		q.Enqueue(record(i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := q.Shutdown(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want deadline exceeded", err)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("shutdown took %v with a 100ms deadline", took)
	}
}

func TestSinkErrorsAreCounted(t *testing.T) {
	failures := 0
	q := NewQueue(&MemorySink{Err: errors.New("boom")}, 10, Hooks{SinkError: func() { failures++ }}, quiet)
	for i := range 3 {
		q.Enqueue(record(i))
	}
	q.Shutdown(context.Background())
	if failures != 3 {
		t.Errorf("counted %d sink errors, want 3", failures)
	}
}

func TestJSONLSink(t *testing.T) {
	path := filepath.Join(t.TempDir(), "captures.jsonl")
	sink, err := NewJSONLSink(path)
	if err != nil {
		t.Fatal(err)
	}
	q := NewQueue(sink, 10, Hooks{}, quiet)
	for i := range 3 {
		rec := record(i)
		rec.RequestBody = []byte(`{"model":"m"}`)
		q.Enqueue(rec)
	}
	if err := q.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	lines := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var rec capturepb.CaptureRecord
		if err := protojson.Unmarshal(scanner.Bytes(), &rec); err != nil {
			t.Fatalf("line %d is not a capture record: %v", lines, err)
		}
		if rec.RequestId != fmt.Sprintf("req-%d", lines) || string(rec.RequestBody) != `{"model":"m"}` {
			t.Errorf("line %d = %v", lines, &rec)
		}
		lines++
	}
	if lines != 3 {
		t.Errorf("file has %d lines, want 3", lines)
	}
}
