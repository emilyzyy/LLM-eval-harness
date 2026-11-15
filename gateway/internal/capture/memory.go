package capture

import (
	"context"
	"sync"
	"time"

	"github.com/emilyzyy/LLM-eval-harness/gateway/gen/go/capturepb"
)

// MemorySink keeps records in memory. It exists for tests, which can also
// make it slow or failing to see how the gateway copes.
type MemorySink struct {
	// Delay makes every Write take this long.
	Delay time.Duration
	// Err, if set, is returned by every Write and the record is not kept.
	Err error

	mu      sync.Mutex
	records []*capturepb.CaptureRecord
}

func (s *MemorySink) Write(_ context.Context, rec *capturepb.CaptureRecord) error {
	if s.Delay > 0 {
		time.Sleep(s.Delay)
	}
	if s.Err != nil {
		return s.Err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, rec)
	return nil
}

func (s *MemorySink) Flush(context.Context) error { return nil }

func (s *MemorySink) Close() error { return nil }

// Records returns a copy of the list of records written so far.
func (s *MemorySink) Records() []*capturepb.CaptureRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*capturepb.CaptureRecord(nil), s.records...)
}
