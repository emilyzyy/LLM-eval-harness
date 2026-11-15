# Gateway

An OpenAI-compatible proxy that sits between clients and a model provider. It
forwards `POST /v1/chat/completions` (plain and streaming), and publishes a
record of every request and response to a Kafka topic.

This is the first layer of a shadow-testing system. The records on the topic
are what later layers will replay against candidate models and score. Those
layers (shadow workers, scoring, dashboard) are not built yet. See
[DESIGN.md](DESIGN.md) for why it is built the way it is.

It is a separate Go module. It does not import or change the Python harness in
the rest of this repository.

## Run it

You need Docker.

```sh
cd gateway
docker compose up --build
```

This starts three containers:

| Service | Address | What it is |
|---|---|---|
| `gateway` | `localhost:8080` | the proxy |
| `fakeupstream` | `localhost:9090` | a pretend provider that returns made-up text |
| `redpanda` | `localhost:19092` | a single-node Kafka-compatible broker |

Send a request:

```sh
curl -i localhost:8080/v1/chat/completions \
  -H 'Authorization: Bearer local-proxy-key' \
  -H 'Content-Type: application/json' \
  -d '{"model":"fake-model","messages":[{"role":"user","content":"hello"}]}'
```

Add `"stream": true` to the body and `-N` to curl to watch a stream arrive
chunk by chunk. The response carries an `X-Request-ID` header; the same ID is
in the gateway's log line and is the key of the capture record.

## Point an OpenAI client at it

Set the client's base URL to the gateway and its API key to the **proxy key**.
The client never sees the provider's key: the gateway swaps it in.

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8080/v1", api_key="local-proxy-key")
reply = client.chat.completions.create(
    model="fake-model",
    messages=[{"role": "user", "content": "hello"}],
)
```

Or, for tools that read the environment:

```sh
export OPENAI_BASE_URL=http://localhost:8080/v1
export OPENAI_API_KEY=local-proxy-key
```

To use a real provider, add it under `upstreams` in the config with its base
URL, the name of the environment variable holding its key, and the models to
route to it.

## Read the captures topic

Each message is one `CaptureRecord` (schema: [proto/capture.proto](proto/capture.proto)),
protobuf-encoded, keyed by request ID.

List keys, partitions and offsets:

```sh
docker compose exec redpanda rpk topic consume captures -n 5 -f '%k partition=%p offset=%o\n'
```

Decode one record to JSON (needs [buf](https://buf.build/docs/installation)):

```sh
docker compose exec -T redpanda rpk topic consume captures -n 1 -f '%v' \
  | buf convert proto/capture.proto --type capture.v1.CaptureRecord \
      --from -#format=binpb --to -#format=json
```

`requestBody` and `responseBody` are bytes, which JSON shows as base64. Pipe
the output through `jq -r .responseBody | base64 -d` to read one.

From code, use the generated types: Go in `gen/go/capturepb`, Python in
`gen/python/capture_pb2.py`.

```python
import capture_pb2

record = capture_pb2.CaptureRecord()
record.ParseFromString(message_value)
print(record.request_id, record.model, record.total_latency.ToTimedelta())
```

## Run it without Docker

You need Go. This uses the JSONL sink, which writes captures to a local file
instead of Kafka.

```sh
cd gateway
export GATEWAY_PROXY_KEY=local-proxy-key FAKE_UPSTREAM_KEY=local-upstream-key

go run ./cmd/fakeupstream -addr :9090 &
go run ./cmd/gateway -config config.example.yaml
```

```sh
tail -n 1 captures.jsonl | jq -r .responseBody | base64 -d
```

## Configuration

One YAML file, passed with `-config`. [config.example.yaml](config.example.yaml)
lists every setting with its default. Keys are never written in the file; it
names the environment variables that hold them.

The capture sink is one of:

| `capture.sink` | Records go to |
|---|---|
| `kafka` | a Kafka topic, protobuf-encoded |
| `jsonl` | a local file, one JSON object per line (for debugging) |
| `none` | nowhere (the default) |

## Endpoints

| Endpoint | Purpose |
|---|---|
| `POST /v1/chat/completions` | the proxy; needs `Authorization: Bearer <proxy key>` |
| `GET /healthz` | 200 if the process is running |
| `GET /readyz` | 200 if an upstream is configured; also reports Kafka's state, which does not affect the answer |
| `GET /metrics` | Prometheus metrics |

Statuses the gateway itself produces: 401 (bad proxy key), 400 (body is not
JSON), 404 (no upstream for the model), 413 (body too large), 429 (the
gateway's rate limit, with `Retry-After`), 502 (upstream unreachable), 504
(upstream timed out). Any other status is the upstream's, passed through.

### Metrics

| Metric | Type | Meaning |
|---|---|---|
| `gateway_requests_total{status}` | counter | requests by status sent; 499 = client disconnected first |
| `gateway_request_duration_seconds` | histogram | total latency |
| `gateway_upstream_duration_seconds` | histogram | time waiting on the upstream |
| `gateway_proxy_overhead_seconds` | histogram | total minus upstream |
| `gateway_in_flight_requests` | gauge | requests being handled now |
| `gateway_rate_limited_total{upstream}` | counter | requests rejected with 429 by the gateway |
| `gateway_capture_enqueued_total` | counter | records accepted into the capture queue |
| `gateway_capture_dropped_total` | counter | records dropped because the queue was full |
| `gateway_capture_sink_errors_total` | counter | records the sink refused |
| `gateway_kafka_produce_errors_total` | counter | records Kafka did not acknowledge |

## The fake upstream

`cmd/fakeupstream` answers chat completion requests with `tok0 tok1 ...`.
Nothing in the tests or benchmarks calls a real provider.

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `:9090` | listen address |
| `-seed` | `1` | seed for random latency and errors |
| `-latency-dist` | `fixed` | `fixed`, `uniform` or `exponential` |
| `-latency` | `20ms` | fixed value, exponential mean, or uniform lower bound |
| `-latency-max` | `0` | uniform upper bound |
| `-error-rate` | `0` | share of requests answered with 503 |
| `-tokens` | `16` | completion tokens per response; a stream sends one chunk per token |
| `-chunk-delay` | `10ms` | pause between stream chunks |
| `-require-key` | `$FAKE_UPSTREAM_KEY` | if set, the key callers must send |

With the same seed, the Nth request to arrive always gets the same latency and
the same success or failure.

## Tests

```sh
cd gateway
go test -race ./...                                                    # no Docker needed
go test -race -tags integration -run 'Kafka|Broker' ./internal/proxy/  # starts Redpanda in Docker
```

The Python test decodes a record written by Go, to show the schema works in
both languages. From the repository root:

```sh
pip install "protobuf>=7.36.2" pytest
pytest gateway/schema_test
```

## Changing the schema

Edit `proto/capture.proto`, then regenerate the Go and Python code and the
test fixture. The generated code is checked in, and CI fails if it is stale.

```sh
cd gateway
buf lint
buf generate
go test ./internal/capture -run TestFixture -update
```

Only add fields, with new numbers. Never renumber or reuse one: records already
on the topic were written with the old numbers.

## Benchmark

`bench/run.sh` load-tests the fake upstream directly and through the gateway,
with capture off and with capture publishing to Kafka, and writes a report.
It needs Go, [vegeta](https://github.com/tsenart/vegeta), Docker and Python 3.

```sh
cd gateway
./bench/run.sh
```

Results, with the command and machine they came from, are in
[bench/results/](bench/results/).

## Layout

```
cmd/gateway/            the proxy binary: config, wiring, shutdown
cmd/fakeupstream/       the fake provider binary
internal/config/        YAML config and validation
internal/proxy/         HTTP handlers: auth, routing, rate limit, forwarding, streaming
internal/upstream/      the Upstream interface and its OpenAI-compatible implementation
internal/capture/       the capture queue and the Kafka, JSONL and in-memory sinks
internal/metrics/       Prometheus metrics
internal/fakeupstream/  the fake provider, as a library the tests run in-process
proto/                  the capture record schema
gen/                    Go and Python code generated from the schema
schema_test/            the Python decode test and its fixture
bench/                  the benchmark script and results
```
