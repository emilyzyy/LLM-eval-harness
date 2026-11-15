//go:build integration

// These tests start a real Kafka-compatible broker (Redpanda) in Docker.
// They are skipped by a plain `go test ./...`. Run them with:
//
//	go test -race -tags integration ./internal/proxy/
package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/emilyzyy/LLM-eval-harness/gateway/gen/go/capturepb"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/capture"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/fakeupstream"
	"github.com/emilyzyy/LLM-eval-harness/gateway/internal/metrics"
)

const redpandaImage = "docker.redpanda.com/redpandadata/redpanda:v24.2.4"

func startRedpanda(t *testing.T) (*redpanda.Container, string) {
	t.Helper()
	ctx := context.Background()
	container, err := redpanda.Run(ctx, redpandaImage)
	if err != nil {
		t.Fatalf("starting Redpanda (is Docker running?): %v", err)
	}
	t.Cleanup(func() { testcontainers.TerminateContainer(container) })
	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return container, broker
}

// newKafkaHarness starts a gateway that captures to the given broker.
func newKafkaHarness(t *testing.T, upstreamURL, broker string, kafkaCfg capture.KafkaConfig) *harness {
	t.Helper()
	m := metrics.New()
	kafkaCfg.Brokers = []string{broker}
	kafkaCfg.Topic = "captures"
	kafkaCfg.Partitions = 3
	kafkaCfg.ReplicationFactor = 1
	kafkaCfg.OnProduceError = m.KafkaProduceErr.Inc
	sink, err := capture.NewKafkaSink(kafkaCfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return newHarnessWithSink(t, upstreamURL, m, sink, nil)
}

func TestKafkaRecordsArriveKeyedByRequestID(t *testing.T) {
	_, broker := startRedpanda(t)
	_, url := startFake(t, fakeupstream.Config{Tokens: 3})
	h := newKafkaHarness(t, url, broker, capture.KafkaConfig{
		DeliveryTimeout:    10 * time.Second,
		MaxBufferedRecords: 1000,
	})

	// Send a mix of plain and streamed requests, noting each request ID.
	const requests = 20
	sent := map[string]bool{}
	for i := range requests {
		body := chatBody
		if i%2 == 1 {
			body = streamBody
		}
		resp, _ := h.mustPost(body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		sent[resp.Header.Get("X-Request-ID")] = true
	}

	// Read the topic from the beginning with an ordinary consumer.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics("captures"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	received := map[string]bool{}
	partitions := map[int32]bool{}
	for len(received) < requests {
		fetches := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("only %d of %d records arrived on the topic", len(received), requests)
		}
		fetches.EachRecord(func(kafkaRecord *kgo.Record) {
			var rec capturepb.CaptureRecord
			if err := proto.Unmarshal(kafkaRecord.Value, &rec); err != nil {
				t.Errorf("a record does not decode with the schema: %v", err)
				return
			}
			if string(kafkaRecord.Key) != rec.RequestId {
				t.Errorf("record key %q != request_id %q", kafkaRecord.Key, rec.RequestId)
			}
			if !sent[rec.RequestId] {
				t.Errorf("unexpected record %q", rec.RequestId)
			}
			if received[rec.RequestId] {
				t.Errorf("record %q arrived twice", rec.RequestId)
			}
			if rec.Model != "fake-model" || rec.StatusCode != 200 || rec.Usage.GetCompletionTokens() != 3 {
				t.Errorf("record content is wrong: %v", &rec)
			}
			received[rec.RequestId] = true
			partitions[kafkaRecord.Partition] = true
		})
	}

	// The gateway created the topic with the configured partition count,
	// and the keys spread the records over more than one partition.
	if len(partitions) < 2 {
		t.Errorf("records landed on %d partition(s); expected them spread over several", len(partitions))
	}
	if errs := testutil.ToFloat64(h.metrics.KafkaProduceErr); errs != 0 {
		t.Errorf("%v produce errors with a healthy broker", errs)
	}
	eventually(t, "/readyz to report Kafka as connected", func() bool {
		reporter := h.gateway.sink.(capture.StateReporter)
		return reporter.State() == capture.KafkaConnected
	})
}

// topicCounter reads the captures topic from the start and counts the
// records on it.
type topicCounter struct {
	client  *kgo.Client
	records int
	ids     map[string]int
}

func newTopicCounter(t *testing.T, broker string) *topicCounter {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics("captures"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return &topicCounter{client: client, ids: map[string]int{}}
}

// poll reads whatever has arrived in the next short while.
func (c *topicCounter) poll() {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c.client.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
		c.records++
		c.ids[string(r.Key)]++
	})
}

// captureAccounts is what the gateway's counters say happened to its records.
type captureAccounts struct {
	handled, dropped, produceErrors, sinkErrors float64
}

func accountsOf(m *metrics.Metrics) captureAccounts {
	return captureAccounts{
		// Every request in these tests reaches the upstream and gets a 200,
		// so each one produces exactly one capture record.
		handled:       testutil.ToFloat64(m.Requests.WithLabelValues("200")),
		dropped:       testutil.ToFloat64(m.CaptureDropped),
		produceErrors: testutil.ToFloat64(m.KafkaProduceErr),
		sinkErrors:    testutil.ToFloat64(m.CaptureSinkError),
	}
}

// lost is every record the gateway counted as not stored.
func (a captureAccounts) lost() float64 { return a.dropped + a.produceErrors + a.sinkErrors }

func (a captureAccounts) String() string {
	return fmt.Sprintf("handled %v, counted lost %v (queue full %v, produce errors %v, sink errors %v)",
		a.handled, a.lost(), a.dropped, a.produceErrors, a.sinkErrors)
}

// TestBrokerOutageDoesNotAffectClients measures client latency with the
// broker up, stops the broker, and measures again. It also checks that every
// request handled is accounted for: its record is either on the topic or
// counted as lost.
func TestBrokerOutageDoesNotAffectClients(t *testing.T) {
	container, broker := startRedpanda(t)
	const upstreamLatency = 20 * time.Millisecond
	_, url := startFake(t, fakeupstream.Config{Latency: upstreamLatency})
	h := newKafkaHarness(t, url, broker, capture.KafkaConfig{
		DeliveryTimeout: 2 * time.Second,
		// A small buffer, so the outage fills it during the test.
		MaxBufferedRecords: 20,
	})

	// run sends n requests and returns how many succeeded and the latencies.
	run := func(n int) (ok int, latencies []time.Duration) {
		for range n {
			start := time.Now()
			resp, _ := h.mustPost(chatBody)
			latencies = append(latencies, time.Since(start))
			if resp.StatusCode == http.StatusOK {
				ok++
			}
		}
		slices.Sort(latencies)
		return ok, latencies
	}

	const requests = 100
	okBefore, before := run(requests)

	// Before the outage, wait until every record so far is either on the
	// topic or counted as lost. Then nothing is in flight when the broker
	// stops, and the topic count cannot change afterwards. (A record the
	// broker stored but could not acknowledge before it stopped would be
	// both on the topic and counted lost. Waiting here keeps that case out
	// of the test; the topic cannot be read after the outage to look for it,
	// because a restarted Redpanda container gets a new port that its
	// advertised address does not match.)
	topic := newTopicCounter(t, broker)
	deadline := time.Now().Add(15 * time.Second)
	for {
		topic.poll()
		a := accountsOf(h.metrics)
		if float64(topic.records)+a.lost() == a.handled {
			t.Logf("before the outage: %v; on topic %d", a, topic.records)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("before the outage, records did not add up: %v; on topic %d", a, topic.records)
		}
	}
	if len(topic.ids) != topic.records {
		t.Errorf("%d records on the topic but %d distinct request IDs: duplicates", topic.records, len(topic.ids))
	}

	stopTimeout := time.Second
	if err := container.Stop(context.Background(), &stopTimeout); err != nil {
		t.Fatalf("stopping the broker: %v", err)
	}

	okDuring, during := run(requests)

	median := func(sorted []time.Duration) time.Duration { return sorted[len(sorted)/2] }
	t.Logf("broker up:   %d/%d ok, median %v, max %v", okBefore, requests, median(before), before[len(before)-1])
	t.Logf("broker down: %d/%d ok, median %v, max %v", okDuring, requests, median(during), during[len(during)-1])

	if okBefore != requests || okDuring != requests {
		t.Errorf("success: %d/%d before the outage, %d/%d during", okBefore, requests, okDuring, requests)
	}
	// Each request takes about 20ms in the upstream. Allow generous room
	// for a busy CI machine while still catching any wait on Kafka, which
	// would cost seconds.
	if diff := median(during) - median(before); diff > 10*time.Millisecond {
		t.Errorf("median latency rose by %v during the outage", diff)
	}
	if slowest := during[len(during)-1]; slowest > upstreamLatency+200*time.Millisecond {
		t.Errorf("slowest request during the outage took %v", slowest)
	}

	// The lost records are counted, not silently discarded.
	eventually(t, "produce errors to be counted", func() bool {
		return testutil.ToFloat64(h.metrics.KafkaProduceErr) > 0
	})

	// Stop the HTTP server first, as main does. That waits for the handlers
	// to return, and a handler enqueues its record after the client already
	// has the response. Without this, the last record could be enqueued
	// after the queue had shut down, and would never be counted.
	h.server.Close()

	// Shutdown must respect its deadline even though nothing can be flushed.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	h.gateway.Shutdown(ctx)
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("shutdown took %v with a 1s deadline and no broker", took)
	}

	// After shutdown the Kafka client has failed every record it still held,
	// so every request is accounted for: handled = on topic + counted lost.
	// Nothing can have reached the topic since the broker stopped. The
	// client reports its failures from a goroutine of its own, so the last
	// counts can land just after Shutdown returns.
	var a captureAccounts
	deadline = time.Now().Add(5 * time.Second)
	for {
		a = accountsOf(h.metrics)
		if float64(topic.records)+a.lost() == a.handled || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("after the outage: %v; on topic %d", a, topic.records)
	if a.handled != 2*requests {
		t.Errorf("the gateway counted %v requests handled, want %d", a.handled, 2*requests)
	}
	if float64(topic.records)+a.lost() != a.handled {
		t.Errorf("records do not add up: on topic %d + counted lost %v != handled %v (%v)",
			topic.records, a.lost(), a.handled, a)
	}
}
