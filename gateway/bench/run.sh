#!/usr/bin/env bash
# Load-tests the fake upstream directly and through the gateway, and writes
# the results to bench/results/<date>/.
#
#   ./bench/run.sh
#   RATES="500 1000" DURATION=10s ./bench/run.sh    # a quicker run
#
# Needs: go, vegeta, docker, python3. Everything runs on this machine: the
# load generator, the fake upstream and the gateways as local processes, and
# Redpanda in Docker. No real provider is contacted.
#
# Three targets are measured at every rate:
#   direct        vegeta -> fake upstream
#   proxy         vegeta -> gateway (capture off)   -> fake upstream
#   proxy_kafka   vegeta -> gateway (capture: kafka) -> fake upstream
set -euo pipefail
cd "$(dirname "$0")/.."

RATES=${RATES:-"1000 2000 4000 8000 12000 16000 24000 32000"}   # requests per second
DURATION=${DURATION:-20s}                           # per run
COOLDOWN=${COOLDOWN:-5}                             # seconds between runs
OUT=${OUT:-bench/results/$(date +%Y-%m-%d)}
COMMAND="RATES=\"$RATES\" DURATION=$DURATION COOLDOWN=$COOLDOWN ./bench/run.sh"

# Fake upstream behaviour. Latency is fixed so that any change in a
# percentile comes from the gateway and not from the upstream's randomness.
UPSTREAM_LATENCY=20ms
UPSTREAM_TOKENS=16
UPSTREAM_CHUNK_DELAY=2ms

REDPANDA_IMAGE=docker.redpanda.com/redpandadata/redpanda:v24.2.4
REDPANDA_NAME=gateway-bench-redpanda
FAKE_ADDR=127.0.0.1:19090
PROXY_ADDR=127.0.0.1:18081
PROXY_KAFKA_ADDR=127.0.0.1:18082
KAFKA_ADDR=127.0.0.1:19092

WORK=$(mktemp -d)
PIDS=""
cleanup() {
  for pid in $PIDS; do kill "$pid" 2>/dev/null || true; done
  docker rm -f "$REDPANDA_NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

# Each open connection uses a file descriptor; the default limit is too low
# for the higher rates.
ulimit -n 10240 2>/dev/null || true

mkdir -p "$OUT/raw"
rm -f "$OUT"/raw/*

echo "== building"
go build -o "$WORK/bin/" ./cmd/gateway ./cmd/fakeupstream

echo "== starting Redpanda"
docker rm -f "$REDPANDA_NAME" >/dev/null 2>&1 || true
docker run -d --name "$REDPANDA_NAME" -p 19092:19092 "$REDPANDA_IMAGE" \
  redpanda start --mode=dev-container --smp=1 \
  --kafka-addr=0.0.0.0:19092 --advertise-kafka-addr="$KAFKA_ADDR" >/dev/null
until docker exec "$REDPANDA_NAME" rpk cluster health --exit-when-healthy >/dev/null 2>&1; do sleep 1; done

echo "== starting the fake upstream and two gateways"
write_config() {  # write_config <file> <listen address> <sink>
  cat > "$1" <<YAML
listen_addr: "$2"
proxy_key_env: GATEWAY_PROXY_KEY
upstreams:
  - name: fake
    base_url: http://$FAKE_ADDR
    api_key_env: FAKE_UPSTREAM_KEY
    models: [fake-model]
capture:
  sink: $3
  kafka:
    brokers: ["$KAFKA_ADDR"]
YAML
}
write_config "$WORK/proxy.yaml" "$PROXY_ADDR" none
write_config "$WORK/proxy_kafka.yaml" "$PROXY_KAFKA_ADDR" kafka
cp "$WORK/proxy.yaml" "$OUT/raw/config-proxy.yaml"
cp "$WORK/proxy_kafka.yaml" "$OUT/raw/config-proxy_kafka.yaml"

export GATEWAY_PROXY_KEY=bench-key FAKE_UPSTREAM_KEY=bench-key
"$WORK/bin/fakeupstream" -addr "$FAKE_ADDR" -seed 1 -latency-dist fixed -latency "$UPSTREAM_LATENCY" \
  -tokens "$UPSTREAM_TOKENS" -chunk-delay "$UPSTREAM_CHUNK_DELAY" >/dev/null 2>&1 &
PIDS="$PIDS $!"
# Gateway logs are discarded: one log line per request to a terminal or file
# would measure the disk rather than the gateway.
"$WORK/bin/gateway" -config "$WORK/proxy.yaml" >/dev/null 2>&1 &
PIDS="$PIDS $!"
"$WORK/bin/gateway" -config "$WORK/proxy_kafka.yaml" >/dev/null 2>&1 &
PIDS="$PIDS $!"

sleep 1
for pid in $PIDS; do
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "a process failed to start; is one of the ports $FAKE_ADDR, $PROXY_ADDR, $PROXY_KAFKA_ADDR in use?" >&2
    exit 1
  fi
done
until curl -sf "http://$FAKE_ADDR/healthz" >/dev/null; do sleep 0.2; done
until curl -sf "http://$PROXY_ADDR/readyz" >/dev/null; do sleep 0.2; done
# Wait until the Kafka gateway has reached the broker and created the topic.
until curl -sf "http://$PROXY_KAFKA_ADDR/readyz" | grep -q '"kafka":"connected"'; do sleep 0.5; done

echo "== recording machine details"
{
  echo "date:            $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "command:         $COMMAND"
  echo "git commit:      $(git rev-parse HEAD 2>/dev/null || echo unknown) $(git status --porcelain . 2>/dev/null | grep -q . && echo '(with uncommitted changes under gateway/)')"
  echo "os:              $(uname -srm)"
  if [ "$(uname -s)" = Darwin ]; then
    echo "os version:      macOS $(sw_vers -productVersion)"
    echo "cpu:             $(sysctl -n machdep.cpu.brand_string)"
    echo "cpu cores:       $(sysctl -n hw.ncpu)"
    echo "memory:          $(( $(sysctl -n hw.memsize) / 1073741824 )) GiB"
  else
    echo "cpu:             $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
    echo "cpu cores:       $(nproc)"
    echo "memory:          $(awk '/MemTotal/ {printf "%d GiB", $2/1048576}' /proc/meminfo)"
  fi
  echo "go:              $(go version | cut -d' ' -f3-)"
  echo "vegeta:          $(vegeta -version | head -1)"
  echo "docker:          $(docker info --format '{{.ServerVersion}}, {{.NCPU}} CPUs and {{.MemTotal}} bytes of memory available to containers')"
  echo "redpanda:        $REDPANDA_IMAGE (--mode=dev-container --smp=1)"
  echo "fake upstream:   fixed latency $UPSTREAM_LATENCY, $UPSTREAM_TOKENS tokens, stream chunk delay $UPSTREAM_CHUNK_DELAY, seed 1"
  echo "gateway config:  defaults, except as in raw/config-*.yaml (no rate limit; logs discarded)"
  echo "run length:      $DURATION per run, ${COOLDOWN}s between runs"
  echo "open file limit: $(ulimit -n)"
} > "$OUT/machine.txt"
cat "$OUT/machine.txt"

echo '{"model":"fake-model","messages":[{"role":"user","content":"Summarise the design review notes."}]}' > "$WORK/nonstream.json"
echo '{"model":"fake-model","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"Summarise the design review notes."}]}' > "$WORK/stream.json"

address_of() {
  case "$1" in
    direct) echo "$FAKE_ADDR" ;;
    proxy) echo "$PROXY_ADDR" ;;
    proxy_kafka) echo "$PROXY_KAFKA_ADDR" ;;
  esac
}

# Check that each target answers a request correctly before loading it.
for target in direct proxy proxy_kafka; do
  status=$(curl -s -o /dev/null -w '%{http_code}' -H 'Authorization: Bearer bench-key' \
    -d @"$WORK/nonstream.json" "http://$(address_of "$target")/v1/chat/completions")
  if [ "$status" != 200 ]; then
    echo "target $target answered a test request with HTTP $status" >&2
    exit 1
  fi
done

for mode in nonstream stream; do
  for rate in $RATES; do
    for target in direct proxy proxy_kafka; do
      addr=$(address_of "$target")
      name="$mode-$target-$rate"
      # Once a target has failed at some rate, higher rates tell us nothing
      # new, and the wreckage (thousands of half-closed sockets) would
      # disturb the runs that follow.
      if [ -e "$WORK/failed-$mode-$target" ]; then
        echo "== $name skipped: $target already failed at a lower rate"
        continue
      fi
      printf 'POST http://%s/v1/chat/completions\nAuthorization: Bearer bench-key\nContent-Type: application/json\n@%s\n' \
        "$addr" "$WORK/$mode.json" > "$WORK/targets.txt"

      if [ "$target" != direct ]; then curl -s "http://$addr/metrics" > "$WORK/before.prom"; fi

      echo "== $name"
      vegeta attack -rate="$rate" -duration="$DURATION" -timeout=10s -targets="$WORK/targets.txt" \
        | vegeta report -type=json > "$OUT/raw/$name.json"

      # Let the capture queue and the Kafka client finish what they hold
      # before reading the counters, and let the machine settle.
      sleep "$COOLDOWN"
      if [ "$target" != direct ]; then
        curl -s "http://$addr/metrics" > "$WORK/after.prom"
        python3 bench/summarize.py delta "$WORK/before.prom" "$WORK/after.prom" > "$OUT/raw/$name.metrics.json"
      fi

      if ! python3 bench/summarize.py sustained "$OUT/raw/$name.json" "$rate"; then
        echo "== $name did not sustain the rate; pausing 30s for sockets to clear"
        touch "$WORK/failed-$mode-$target"
        sleep 30
      fi
    done
  done
done

python3 bench/summarize.py table "$OUT" > "$OUT/RESULTS.md"
echo "== wrote $OUT/RESULTS.md"
