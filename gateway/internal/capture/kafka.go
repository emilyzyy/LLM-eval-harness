package capture

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/emilyzyy/LLM-eval-harness/gateway/gen/go/capturepb"
)

// KafkaConfig is what the Kafka sink needs to know.
type KafkaConfig struct {
	Brokers            []string
	Topic              string
	Partitions         int32
	ReplicationFactor  int16
	DeliveryTimeout    time.Duration
	MaxBufferedRecords int
	// Called once for every record Kafka did not acknowledge. May be nil.
	OnProduceError func()
}

// Kafka connection states reported on /readyz.
const (
	KafkaUnknown     = "unknown"
	KafkaConnected   = "connected"
	KafkaUnreachable = "unreachable"
)

// KafkaSink publishes each record to a topic as protobuf bytes, with the
// request ID as the message key.
//
// Write is asynchronous. It puts the record in the Kafka client's own buffer
// and returns; the client sends batches in the background and reports each
// record's result to a callback. A failed record is counted and logged. It is
// not retried here: the client has already retried for DeliveryTimeout.
type KafkaSink struct {
	client *kgo.Client
	cfg    KafkaConfig
	logger *slog.Logger

	state atomic.Value // one of the Kafka* state strings

	stop     chan struct{} // closed by Close to end the watch goroutine
	stopOnce sync.Once
	watching sync.WaitGroup

	// unresolved counts records handed to the client whose result has not
	// come back yet. Close uses it to count the records it abandons.
	mu         sync.Mutex
	unresolved int
	closed     bool
}

// NewKafkaSink creates the sink. It does not connect: the client connects in
// the background when it first has something to do, so the gateway starts
// even if Kafka is down.
func NewKafkaSink(cfg KafkaConfig, logger *slog.Logger) (*KafkaSink, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		// The idempotent producer is franz-go's default, and it requires
		// acks from all in-sync replicas. Both are set here anyway so the
		// delivery guarantee is visible in the code. With idempotence the
		// broker discards a batch it has already stored, so the client's own
		// retries cannot create duplicate records.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Give up on a record the broker has not acknowledged in time,
		// rather than holding it forever.
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
		// Bound the memory used while the broker is slow or down.
		kgo.MaxBufferedRecords(cfg.MaxBufferedRecords),
	)
	if err != nil {
		return nil, err
	}

	s := &KafkaSink{client: client, cfg: cfg, logger: logger, stop: make(chan struct{})}
	s.state.Store(KafkaUnknown)
	s.watching.Add(1)
	go s.watch()
	return s, nil
}

// Write queues the record in the Kafka client and returns without waiting for
// the broker.
func (s *KafkaSink) Write(ctx context.Context, rec *capturepb.CaptureRecord) error {
	value, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	record := &kgo.Record{Key: []byte(rec.GetRequestId()), Value: value}

	s.mu.Lock()
	s.unresolved++
	s.mu.Unlock()

	// TryProduce never blocks. If the client's buffer is full it fails the
	// record straight away, through the same callback as any other failure.
	s.client.TryProduce(ctx, record, func(_ *kgo.Record, err error) {
		s.mu.Lock()
		alreadyCounted := s.closed // Close counted this record as abandoned
		s.unresolved--
		s.mu.Unlock()
		if err == nil || alreadyCounted {
			return
		}
		s.produceError()
		s.logger.Warn("kafka produce failed", "request_id", rec.GetRequestId(), "error", err)
	})
	return nil
}

func (s *KafkaSink) produceError() {
	if s.cfg.OnProduceError != nil {
		s.cfg.OnProduceError()
	}
}

// Flush waits until every buffered record has been acknowledged or has
// failed, or until ctx ends.
func (s *KafkaSink) Flush(ctx context.Context) error {
	return s.client.Flush(ctx)
}

// Close stops the background goroutine and closes the Kafka client. Records
// still buffered are abandoned.
func (s *KafkaSink) Close() error {
	s.stopOnce.Do(func() { close(s.stop) })
	s.watching.Wait()
	s.client.Close()

	// A record the client sent just before the broker went away has no
	// result: the client cannot tell whether the broker stored it, so it
	// never reports it as failed. Count such records as produce errors here,
	// so that every record is either acknowledged or counted. The count can
	// include a record that did reach the topic.
	s.mu.Lock()
	abandoned := s.unresolved
	s.closed = true
	s.mu.Unlock()
	for range abandoned {
		s.produceError()
	}
	if abandoned > 0 {
		s.logger.Warn("kafka records abandoned at shutdown without a result", "records", abandoned)
	}
	return nil
}

// State reports whether a broker answered the most recent check.
func (s *KafkaSink) State() string {
	return s.state.Load().(string)
}

// watch checks every few seconds whether Kafka is reachable, and creates the
// topic the first time it is. It only feeds /readyz and the log; producing
// does not depend on it.
func (s *KafkaSink) watch() {
	defer s.watching.Done()

	const interval = 5 * time.Second
	topicReady := false
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := s.client.Ping(ctx)
		if err == nil && !topicReady {
			err = s.ensureTopic(ctx)
			topicReady = err == nil
		}
		cancel()

		newState := KafkaConnected
		if err != nil {
			newState = KafkaUnreachable
		}
		if old := s.state.Swap(newState); old != newState {
			if err != nil {
				s.logger.Warn("kafka is unreachable", "error", err)
			} else {
				s.logger.Info("kafka is connected")
			}
		}

		select {
		case <-s.stop:
			return
		case <-time.After(interval):
		}
	}
}

// ensureTopic creates the topic if it does not exist yet.
func (s *KafkaSink) ensureTopic(ctx context.Context) error {
	admin := kadm.NewClient(s.client)
	_, err := admin.CreateTopic(ctx, s.cfg.Partitions, s.cfg.ReplicationFactor, nil, s.cfg.Topic)
	if errors.Is(err, kerr.TopicAlreadyExists) {
		return nil
	}
	return err
}
