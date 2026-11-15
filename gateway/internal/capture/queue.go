package capture

import (
	"context"
	"log/slog"
	"sync"

	"github.com/emilyzyy/LLM-eval-harness/gateway/gen/go/capturepb"
)

// Hooks lets the caller count what happens to records. Any field may be nil.
type Hooks struct {
	// Called when a record is accepted into the queue.
	Enqueued func()
	// Called when a record is dropped because the queue is full.
	Dropped func()
	// Called when the sink returns an error for a record.
	SinkError func()
}

// Queue is a bounded buffer between request handlers and a Sink.
type Queue struct {
	records chan *capturepb.CaptureRecord
	sink    Sink
	hooks   Hooks
	logger  *slog.Logger

	stop     chan struct{} // closed by Shutdown to tell the writer to finish
	stopOnce sync.Once
	done     chan struct{} // closed by the writer when it has finished
}

// NewQueue creates a queue holding up to size records and starts its
// background writer.
func NewQueue(sink Sink, size int, hooks Hooks, logger *slog.Logger) *Queue {
	q := &Queue{
		records: make(chan *capturepb.CaptureRecord, size),
		sink:    sink,
		hooks:   hooks,
		logger:  logger,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go q.run()
	return q
}

// Enqueue hands a record to the background writer and returns immediately.
// It reports false if the queue was full and the record was dropped.
func (q *Queue) Enqueue(rec *capturepb.CaptureRecord) bool {
	// A select with a default case never blocks: if the channel has no room,
	// the default case runs.
	select {
	case q.records <- rec:
		call(q.hooks.Enqueued)
		return true
	default:
		call(q.hooks.Dropped)
		return false
	}
}

// run is the background writer. It is the only goroutine that calls the sink.
func (q *Queue) run() {
	defer close(q.done)
	for {
		select {
		case rec := <-q.records:
			q.write(rec)
		case <-q.stop:
			// Write whatever is already queued, then finish.
			for {
				select {
				case rec := <-q.records:
					q.write(rec)
				default:
					return
				}
			}
		}
	}
}

func (q *Queue) write(rec *capturepb.CaptureRecord) {
	// The sink gets a context that is never cancelled: a record must not be
	// tied to the request that produced it, which has already finished.
	if err := q.sink.Write(context.Background(), rec); err != nil {
		call(q.hooks.SinkError)
		q.logger.Warn("capture sink write failed", "request_id", rec.GetRequestId(), "error", err)
	}
}

// Shutdown writes the records still in the queue, flushes the sink and closes
// it. It gives up when ctx ends, so a stuck sink cannot hold up shutdown; the
// records not yet stored are then lost.
//
// Call it after the HTTP server has stopped, so that no handler is still
// enqueueing.
func (q *Queue) Shutdown(ctx context.Context) error {
	q.stopOnce.Do(func() { close(q.stop) })

	var err error
	select {
	case <-q.done:
		err = q.sink.Flush(ctx)
	case <-ctx.Done():
		// The writer is stuck inside the sink.
		err = ctx.Err()
	}
	if closeErr := q.sink.Close(); err == nil {
		err = closeErr
	}
	return err
}

func call(f func()) {
	if f != nil {
		f()
	}
}
