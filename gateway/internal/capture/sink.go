// Package capture gets capture records from the request handlers to wherever
// they are stored, without ever making a handler wait.
//
// A handler calls Queue.Enqueue, which either puts the record in a bounded
// channel or drops it. One background goroutine takes records off the channel
// and gives them to a Sink.
package capture

import (
	"context"

	"github.com/emilyzyy/LLM-eval-harness/gateway/gen/go/capturepb"
)

// Sink stores capture records. Only the queue's background goroutine calls
// Write, so a Sink does not need to handle concurrent Writes.
type Sink interface {
	// Write stores one record, or starts storing it. A slow Write delays
	// later records, never a client.
	Write(ctx context.Context, rec *capturepb.CaptureRecord) error
	// Flush waits until every record written so far is stored, or ctx ends.
	Flush(ctx context.Context) error
	// Close releases the sink's resources. It is called once, after Flush.
	Close() error
}

// StateReporter is implemented by sinks that depend on an outside service and
// can say whether it is reachable. The gateway shows this on /readyz.
type StateReporter interface {
	State() string
}
