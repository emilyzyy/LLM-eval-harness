# Gateway design

This document explains each decision in the gateway: what was chosen, why, what
was rejected, and what it costs. The last section lists known limitations.

## What this layer does

The end goal is shadow testing: take real traffic, replay it against a
candidate model, and compare the candidate's answers with the ones production
gave. That needs a faithful record of real traffic. This layer produces it.

```
client ──► gateway ──► upstream (model provider)
              │
              │  after the response is finished
              ▼
        capture queue ──► background writer ──► sink (Kafka topic "captures")
                                                        │
                                                        ▼
                                          later: shadow workers, scoring, dashboard
```

A request goes through these steps, in `internal/proxy/handler.go` and
`forward.go`:

1. Assign a request ID.
2. Check the client's proxy key.
3. Read the body and pull out two fields: `model` and `stream`.
4. Pick the upstream for that model and take a token from its rate limiter.
5. Send the request to the upstream and relay the response.
6. Record metrics, write one log line, and hand a capture record to the queue.

Step 6 happens after the client has its response.

## Decisions

### 1. Capture is off the request path

**Decision.** The handler never writes to Kafka. It puts the record in an
in-memory queue (a Go channel) and returns. One background goroutine takes
records off the queue and gives them to the sink.

**Why.** The gateway is in the path of production traffic. Capture exists to
feed an experiment. An experiment must not be able to slow down or break
production. If the handler waited for Kafka, then Kafka's latency would be
added to every request, and a Kafka outage would become a client outage.

**Rejected.**

- *Produce synchronously in the handler and wait for the acknowledgement.*
  Gives the strongest guarantee that a record is stored, but ties client
  latency and availability to Kafka.
- *Start a new goroutine per record.* It does not block the handler, but
  nothing limits how many pile up when the sink is slow. Memory grows until
  the process is killed. A bounded queue makes the limit explicit.
- *Write to a local disk log first, and ship it to Kafka separately.* This is
  the way to get durable capture without blocking (it is what log shippers
  do). It was left out because it adds a second component, disk management
  and replay logic, and shadow testing does not need every record.

**Evidence.** `TestBrokerOutageDoesNotAffectClients` stops the broker in the
middle of a run and checks that latency and success rate do not change, and
that every request handled is accounted for: records on the topic plus
records counted as lost equal requests handled.
`TestCaptureOverflowDropsWithoutSlowingResponses` uses a sink that takes 200 ms
per record and checks that no request waits for it.

### 2. When the queue is full, drop the record

**Decision.** `Queue.Enqueue` never waits. If the queue is full it drops the
record and increments `gateway_capture_dropped_total`.

**Why.** When the sink cannot keep up, something has to give. There are only
three options: block the request (hurts clients), buffer without limit (the
process runs out of memory and every request fails), or drop. A lost capture
record costs one sample from an experiment. A blocked or crashed gateway costs
a production request. Dropping is the cheapest failure, and the counter makes
it visible instead of silent.

**What it costs.** The captured traffic is a sample, not a complete log. The
sample can be biased: drops happen when the gateway is busiest, so peak-time
traffic is under-represented. Anything computed from the topic should be read
with the drop counter next to it.

**Rejected.**

- *Block for a short time before dropping.* It trades a little client latency
  for fewer drops. It also makes latency depend on the sink again, which is
  the thing decision 1 removes.
- *Drop the oldest record instead of the newest.* Neither is more valuable
  here, and dropping the newest is one non-blocking channel send.

The Kafka client has its own bounded buffer (`max_buffered_records`). The
sink calls `TryProduce`, which fails a record immediately when that buffer is
full, rather than `Produce`, which would wait. So there are two bounded
stages, and neither one blocks.

### 3. Delivery guarantee: at most once, and what consumers must do

**Decision.** A capture record reaches the topic zero or one times. The
gateway never tries again after giving up on a record.

A record is lost if:

- the capture queue was full (counted in `gateway_capture_dropped_total`);
- Kafka did not acknowledge it within `delivery_timeout`, or the Kafka
  client's buffer was full (counted in `gateway_kafka_produce_errors_total`);
- the process crashed or was killed with records still in memory (not
  counted, because the process that would count them is gone);
- shutdown reached its `flush_timeout` before everything was written
  (counted in `gateway_kafka_produce_errors_total`: at shutdown the sink
  counts every record that still has no result from the broker).

For a graceful run this gives an accounting rule: requests that reached an
upstream = records on the topic + records counted as lost. The count of lost
records can be slightly too high. A record sent just as the broker went away
has an unknown outcome, so it is counted as lost at shutdown even if the
broker did store it. `TestBrokerOutageDoesNotAffectClients` checks the rule.

Within those limits the producer is configured for safety: it waits for
acknowledgement from all in-sync replicas (`acks=all`), and it is idempotent.
Idempotent means each batch carries a sequence number, so if the client
re-sends a batch because an acknowledgement got lost, the broker recognises
it and does not store it twice. Without this, the client's own retries could
duplicate records.

**What this means for a consumer.**

- *Expect gaps.* Not every production request is on the topic. Do not treat
  the topic as an audit log or use it for billing.
- *Still handle duplicates.* The gateway should not produce them, but Kafka
  consumers normally process a message and then commit their position. A
  consumer that crashes between the two reads the same message again after
  restart. A shadow worker should therefore be idempotent: use `request_id`
  as the key of whatever it writes, so that processing a record twice
  overwrites rather than adds.
- *Do not rely on order between requests.* Records are spread over
  partitions, and Kafka orders messages only within one partition.

**Rejected.** *At-least-once from gateway to topic* would need records to
survive a crash, which means writing them to disk before answering the client
(see decision 1). *Exactly-once* in Kafka refers to transactions that span
reading from one topic and writing to another. It does not apply to a
producer whose input is HTTP requests.

### 4. Records are keyed by request ID

**Decision.** The Kafka message key is the request ID.

**Why.**

- *Even spread.* Kafka picks the partition by hashing the key. Request IDs
  are unique, so records spread evenly and consumers share the work evenly.
- *Same partition for related data later.* When shadow workers publish their
  results, keyed by the same request ID to a topic with the same partition
  count, a request's capture and its shadow results land on the same
  partition number. A scorer can then join them without a shuffle.
- *Deduplication.* The key identifies the request, so a consumer or a
  compacted topic can deduplicate on it.

**Rejected.**

- *Key by model.* Gives per-model ordering, but one popular model would put
  most of the traffic on one partition and one consumer.
- *No key.* Spreads evenly, but loses the co-partitioning and gives
  consumers nothing to deduplicate on.

Ordering across requests is not needed: each request is independent.

### 5. The record is defined in protobuf, and generated code is checked in

**Decision.** `proto/capture.proto` is the single definition of a capture
record. `buf generate` produces Go and Python code from it, and that code is
committed. CI regenerates it and fails if it differs.

**Why protobuf.** The gateway is Go; the eval harness, and probably the
shadow workers, are Python. A schema that both compile from stops the two
sides drifting apart. Protobuf also has clear rules for changing a schema
without breaking readers: add fields with new numbers, never reuse a number.
Old readers skip fields they do not know.

**Why check in generated code.** Anyone can build and test without installing
buf, and a schema change shows up in review as a diff of the generated code.

**Rejected.** *JSON* needs no tooling, but there is no schema to enforce, the
bodies would need escaping inside it, and records are larger. *Avro with a
schema registry* is common with Kafka, but it adds a service to run.

The bodies are `bytes`, not `string`, because a body cut at the capture limit
can end in the middle of a multi-byte character, and a protobuf `string` must
be valid UTF-8.

### 6. The request body is forwarded unchanged

**Decision.** The gateway parses only `model` and `stream` from the body. The
bytes it sends upstream are the bytes it received.

**Why.** Providers keep adding request fields. A gateway that decoded the
body into its own struct and re-encoded it would silently drop any field it
did not know about. Forwarding the original bytes means new fields work
without a gateway change. It also means the captured request is exactly what
the provider saw, which is what a replay needs.

### 7. Streaming: forward each chunk immediately, keep a copy

**Decision.** For a streamed response, the gateway reads from the upstream in
a loop. Each time it gets bytes, it writes them to the client, flushes, and
appends them to the capture buffer.

**Why flush.** Go's HTTP server buffers response writes. Without an explicit
flush after each chunk, the client would see tokens arrive in bursts, or all
at the end. `TestStreamingIsIncremental` checks the gaps between chunks at
the client.

**Why capture the raw stream.** The record stores the SSE text exactly as
sent, rather than a reassembled message. That keeps the gateway out of the
business of understanding every provider's chunk format, and keeps the
chunk boundaries for anyone who later wants to study them.

**A broken stream is cut, not ended.** If the upstream fails or goes quiet
part-way, some of the stream has already been sent and cannot be withdrawn.
The gateway then aborts the connection. If it ended the response normally,
the client could mistake half an answer for a whole one.

### 8. A client disconnect cancels the upstream call

**Decision.** The upstream call uses a context derived from the incoming
request's context.

**Why.** Go cancels a request's context when its client disconnects.
Deriving the upstream call's context from it means the cancellation carries
through: the upstream connection is closed and the provider stops generating.
Otherwise the provider would keep producing, and billing for, tokens nobody
will read. `TestClientCancelStopsUpstream` checks this.

### 9. Timeouts differ for plain and streamed requests

**Decision.**

| Timeout | Applies to | Default |
|---|---|---|
| `request` | a plain request, start to finish, retries included | 60s |
| `response_header` | how long the upstream may take to start answering | 30s |
| `stream_idle` | the longest gap between two stream chunks | 30s |

**Why.** One overall deadline suits a plain request. It does not suit a
stream: a long answer can legitimately take minutes, so any fixed total would
cut off valid responses. What marks a dead stream is silence, so a stream is
limited by the gap between chunks.

The HTTP server has no write timeout, for the same reason.

### 10. Retry only before anything has been sent to the client

**Decision.** An upstream call is retried (once, by default) if the
connection failed or the upstream answered 502, 503 or 504. Nothing is
retried once any byte has been written to the client.

**Why the boundary.** Before the first byte, the client cannot tell a retry
happened. After it, a retry would mean sending a second response on top of
part of the first.

**What is not retried.**

- *Timeouts.* The client has already waited the full limit; a retry would
  double it.
- *4xx responses, including the upstream's 429.* The request is wrong, or
  the provider is asking for less traffic. Sending it again helps neither.

When the retries run out, the upstream's last response is passed to the
client as it is.

**What it costs: a retried request may be billed twice by the provider.** A
chat completion is not idempotent. If the connection drops, or the provider's
front end answers 502, 503 or 504, after the model has already started work,
the retry runs the request a second time and the provider can bill both runs.
The gateway cannot tell from its side whether the first attempt was billed.
Setting `retry.max_attempts: 1` rules this out, at the cost of passing every
such failure to the client.

### 11. Rate limiting rejects instead of waiting

**Decision.** Each upstream has a token bucket (`golang.org/x/time/rate`).
One request takes one token. If there is none, the client gets 429 with a
`Retry-After` header straight away.

**Why reject.** A proxy that queues requests when over the limit hides the
problem from clients and holds a connection and memory for every waiting
request. A 429 tells the client to back off, which every OpenAI client
already knows how to do.

**How this differs from the Python limiter in this repository.** The harness
has `DynamoTokenBucket`, which blocks until a token is free and keeps its
state in DynamoDB so all workers share one bucket. That fits a batch job:
the work can wait, and the limit must hold across machines. The gateway
serves interactive traffic, where waiting is worse than an error, and where
a DynamoDB round trip on every request would cost more than the proxying
itself.

**What it costs.** The bucket lives in one process. Run three gateways and
the provider sees three times the configured rate. It also counts requests,
while providers also limit tokens per minute.

### 12. Request IDs are UUIDv7

**Decision.** Each request gets a UUIDv7, unless the client sent an
`X-Request-ID` made only of letters, digits, `-`, `_` and `.` (128
characters at most), in which case that is used. The ID is returned in the
`X-Request-ID` response header, sent to the upstream, written in every log
line for the request, and used as the capture record's ID and Kafka key.

**Why v7.** A UUIDv7 begins with a millisecond timestamp, so IDs sort by
creation time. That makes them easy to scan in logs and friendly to database
indexes if records are later stored in one. The random v4 has neither
property.

**Why validate a client's ID.** The ID is copied into headers and logs. An
unchecked value could contain line breaks and forge log entries.

**What it costs.** A client can send the same ID twice, and then two records
share a key. Consumers that deduplicate on request ID would keep one.

### 13. Clients use a proxy key, not the provider's key

**Decision.** Clients send `Authorization: Bearer <proxy key>`. The gateway
checks it and replaces it with the upstream's key. Both come from environment
variables named in the config.

**Why.** The provider's key stays in one place. Clients cannot leak it, and
it can be changed without touching them.

**Keys are never logged or captured.** The gateway logs no headers and
captures no headers, only bodies. An error from the upstream call is logged
but never shown to the client, because it can contain internal addresses.
`TestKeysAreNeverLoggedOrCaptured` searches the logs and records for both
keys.

### 14. The gateway starts and stays ready without Kafka

**Decision.** The Kafka client connects lazily, in the background. `/readyz`
reports Kafka's state but returns 200 regardless.

**Why.** Readiness decides whether a load balancer sends traffic to this
instance. If it depended on Kafka, a Kafka outage would take every gateway
out of rotation, which is exactly the coupling decision 1 avoids.

### 15. Shutdown order

**Decision.** On SIGTERM: stop accepting connections and wait for in-flight
requests (`shutdown_timeout`), then write out the capture queue and flush the
sink (`flush_timeout`).

**Why this order.** Requests still running produce records. Flushing first
would miss them. Each step has a deadline so that a stuck client or a dead
broker cannot stop the process from exiting.

### 16. The provider sits behind a small interface

**Decision.** The proxy calls an `Upstream` interface with one method,
`ChatCompletions`. The one implementation speaks the OpenAI HTTP API.

**Why.** Most providers offer an OpenAI-compatible endpoint, so one
implementation covers them. One that does not can be added as a second
implementation that translates, without changes to the proxy.

### 17. The fake upstream

**Decision.** Tests and benchmarks use `internal/fakeupstream`, a small HTTP
server that behaves like a provider. Its latency and failures come from a
seeded random generator.

**Why.** Tests against a real provider cost money, need a key in CI, and
fail for reasons unrelated to the code. A fake also makes it possible to
test things a real provider will not do on demand, such as failing 100% of
requests or stalling in the middle of a stream.

**What "deterministic" means here.** The Nth request to arrive gets the Nth
latency and the Nth pass-or-fail from the seeded sequence. With concurrent
requests, which request arrives Nth can vary between runs, but the overall
set of latencies and failures does not.

### 18. Plain responses are held in memory, up to a limit

**Decision.** A non-streaming response is read into memory and then sent in
one write, as long as it is no longer than `max_response_buffer_bytes`
(4 MiB by default). If it is longer, the gateway stops holding it: it sends
the status, headers and what it has read so far, then copies the rest
through as it arrives, flushing after each write. The client still gets the
full body. The capture record keeps only the part that was held, or less if
`capture.max_body_bytes` is lower, and is marked `response_truncated`.

**Why hold the response at all.** A held response goes out with a
`Content-Length`, and if the upstream fails while it is being read, nothing
has reached the client yet, so the client gets a clean 502 or 504.

**Why a limit.** Without one, each in-flight request can take as much memory
as the upstream cares to send, and a few very large responses can exhaust
the process.

**What it costs.** Past the limit, the response behaves like a stream: if
the upstream fails part-way, the status and part of the body have already
been sent, so the gateway cuts the connection (as in decision 7). The
`Content-Length` is passed on only if the upstream sent one. The record has
no token usage, because usage is read from the end of the body, which was
not kept. `TestPlainResponseOverBufferLimit` checks that the client gets the
first part before the upstream has finished, and then the whole body.

### 19. Dependencies

Standard library where it is enough: HTTP server and client, JSON, logging
(`log/slog`), contexts.

| Dependency | Used for | Why not write it |
|---|---|---|
| `golang.org/x/time/rate` | token bucket | the standard implementation, by the Go team |
| `github.com/twmb/franz-go` | Kafka client | pure Go (no C library to build), idempotent by default, non-blocking produce |
| `google.golang.org/protobuf` | encoding records | required by the generated code |
| `github.com/prometheus/client_golang` | metrics | the standard Prometheus client |
| `github.com/google/uuid` | UUIDv7 | small and widely used |
| `go.yaml.in/yaml/v3` | config file | the standard library has no YAML parser |
| `github.com/testcontainers/testcontainers-go` | starting Redpanda in tests | tests only |

## How later layers plug in

- **Shadow workers** consume the `captures` topic in a consumer group. Each
  record holds the exact request body, so a worker can send it to a candidate
  model unchanged. Nothing in the gateway changes.
- **Scoring** compares a shadow result with `response_body` from the capture
  record, joined on `request_id`.
- **A dashboard** reads the Prometheus metrics for the gateway's health, and
  whatever the scorers write for results.
- **Other destinations** for captures implement the three-method `Sink`
  interface in `internal/capture/sink.go`.
- **New record fields** are added to `capture.proto` with new numbers.
  Existing consumers ignore them until they are updated.

## Known limitations

**Capture**

- Captures are lossy by design (decision 3). Losses from a crash are not
  counted anywhere.
- Records contain full prompts and responses, which may include personal or
  confidential data. There is no redaction, sampling, or encryption beyond
  what the Kafka cluster provides. Access to the topic needs to be controlled.
- Bodies over `capture.max_body_bytes` (256 KiB by default) are cut. The
  limit keeps a record under Kafka's default 1 MB message size. A plain
  response over `max_response_buffer_bytes` is also cut in its record
  (decision 18).
- Token usage for a stream is recorded only if the client asked for it with
  `stream_options.include_usage`. The gateway does not add that option,
  because it would change what the client receives.
- Auth failures and requests rejected by the gateway's rate limiter are not
  captured; they never reached a model.

**Proxy**

- Only `POST /v1/chat/completions` is served. Other OpenAI endpoints, such
  as `/v1/models` and `/v1/embeddings`, return 404.
- There is one proxy key for all clients: no per-client keys, quotas or
  usage accounting.
- Rate limits are per gateway process and count requests, not tokens
  (decision 11).
- A retried request may be billed twice by the provider (decision 10).
- A plain response is held in memory before it is sent on, up to
  `max_response_buffer_bytes`; past that it is passed through as it arrives
  (decision 18). Request bodies are limited by `max_body_bytes`.
- Only a few upstream response headers are passed on: `Content-Type`,
  `Cache-Control`, `Retry-After` and `X-RateLimit-*`.
- There is no TLS. The gateway is expected to sit behind a load balancer
  that terminates it.
- `/metrics`, `/healthz` and `/readyz` share the port with the proxy and
  need no key.

**Measurement**

- "Proxy overhead" is total time minus time spent waiting on the upstream.
  Time spent writing to a slow client therefore counts as overhead.
- The benchmark runs the load generator, fake upstream, gateways and broker
  on one machine, so they compete for the same CPUs, and the network is
  loopback. It measures the cost the gateway adds under those conditions. It
  does not predict throughput on a server. See `bench/results/`.
