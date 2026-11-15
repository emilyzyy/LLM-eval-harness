# Gateway benchmark results

Produced by `bench/run.sh`; do not edit by hand. Raw vegeta reports and
counter changes for every run are in `raw/`.

## Setup

```
date:            2026-11-14T19:39:49Z
command:         RATES="1000 2000 4000 8000 12000 16000 24000 32000" DURATION=20s COOLDOWN=5 ./bench/run.sh
git commit:      d89ab2eb229e71d741228c5c1e32400ae498760d (with uncommitted changes under gateway/)
os:              Darwin 25.6.0 arm64
os version:      macOS 26.6.2
cpu:             Apple M4 Pro
cpu cores:       12
memory:          24 GiB
go:              go1.27.4 darwin/arm64
vegeta:          Version: 12.13.0
docker:          29.5.3, 12 CPUs and 8321232896 bytes of memory available to containers
redpanda:        docker.redpanda.com/redpandadata/redpanda:v24.2.4 (--mode=dev-container --smp=1)
fake upstream:   fixed latency 20ms, 16 tokens, stream chunk delay 2ms, seed 1
gateway config:  defaults, except as in raw/config-*.yaml (no rate limit; logs discarded)
run length:      20s per run, 5s between runs
open file limit: 10240
```

Targets: `direct` is the load generator calling the fake upstream; `proxy` goes
through the gateway with capture off; `proxy_kafka` goes through the gateway
with capture publishing to Redpanda.

How to read the columns:

- **achieved** is successful responses per second over the run. **ok** is the share of requests that got a 200.
- **p50/p95/p99** are end-to-end latencies seen by the load generator, in milliseconds. For streams this is the time to the end of the stream.
- **added** is this row's percentile minus the `direct` percentile at the same rate. It compares two separate runs, so differences of a few hundredths of a millisecond are within run-to-run noise and can be negative.
- **overhead** is the mean of the gateway's own `gateway_proxy_overhead_seconds` histogram during the run: time inside the gateway not spent waiting on the upstream. It excludes the extra network hop, which **added** includes.
- **lost captures** is (records dropped at the capture queue + records Kafka did not acknowledge) / requests handled, from the gateway's counters.

## Non-streaming

| rate (req/s) | target | achieved (req/s) | ok | p50 | p95 | p99 | added p50 | added p95 | added p99 | overhead (mean) | lost captures |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000 | direct | 999.0 | 100.0000% | 20.267 | 20.592 | 25.188 | - | - | - | - | - |
| 1000 | proxy | 999.0 | 100.0000% | 20.382 | 20.777 | 23.789 | +0.116 | +0.184 | -1.399 | 0.038 | - |
| 1000 | proxy_kafka | 999.0 | 100.0000% | 20.380 | 20.842 | 24.257 | +0.113 | +0.250 | -0.930 | 0.039 | 0.0000% (0 dropped + 0 failed of 20000) |
| 2000 | direct | 1998.0 | 100.0000% | 20.156 | 20.331 | 21.938 | - | - | - | - | - |
| 2000 | proxy | 1998.0 | 100.0000% | 20.188 | 20.373 | 24.196 | +0.031 | +0.042 | +2.258 | 0.012 | - |
| 2000 | proxy_kafka | 1998.0 | 100.0000% | 20.184 | 20.376 | 23.098 | +0.027 | +0.045 | +1.160 | 0.013 | 0.0000% (0 dropped + 0 failed of 40000) |
| 4000 | direct | 3996.0 | 100.0000% | 20.084 | 20.184 | 21.462 | - | - | - | - | - |
| 4000 | proxy | 3996.0 | 100.0000% | 20.121 | 20.304 | 24.177 | +0.037 | +0.120 | +2.715 | 0.012 | - |
| 4000 | proxy_kafka | 3996.0 | 100.0000% | 20.121 | 20.304 | 22.972 | +0.038 | +0.120 | +1.511 | 0.013 | 0.0000% (0 dropped + 0 failed of 80000) |
| 8000 | direct | 7992.0 | 100.0000% | 20.062 | 20.175 | 25.588 | - | - | - | - | - |
| 8000 | proxy | 7992.0 | 100.0000% | 20.135 | 20.345 | 28.205 | +0.073 | +0.170 | +2.617 | 0.013 | - |
| 8000 | proxy_kafka | 7983.8 | 100.0000% | 20.137 | 20.340 | 28.837 | +0.075 | +0.166 | +3.248 | 0.013 | 0.0000% (0 dropped + 0 failed of 159941) |
| 12000 | direct | 11988.0 | 100.0000% | 20.072 | 20.191 | 23.785 | - | - | - | - | - |
| 12000 | proxy | 11988.0 | 100.0000% | 20.159 | 20.400 | 31.349 | +0.087 | +0.209 | +7.564 | 0.014 | - |
| 12000 | proxy_kafka | 11988.0 | 100.0000% | 20.161 | 20.563 | 33.548 | +0.090 | +0.372 | +9.763 | 0.014 | 0.0000% (0 dropped + 0 failed of 240001) |
| 16000 | direct | 15984.0 | 100.0000% | 20.079 | 20.269 | 28.207 | - | - | - | - | - |
| 16000 | proxy | 15983.9 | 100.0000% | 20.167 | 20.999 | 33.921 | +0.088 | +0.730 | +5.715 | 0.014 | - |
| 16000 | proxy_kafka | 15983.9 | 100.0000% | 20.169 | 21.230 | 41.165 | +0.089 | +0.961 | +12.958 | 0.016 | 0.0000% (0 dropped + 0 failed of 320000) |
| 24000 | direct | 23976.3 | 100.0000% | 20.087 | 21.230 | 31.992 | - | - | - | - | - |
| 24000 | proxy | 1602.1 | 10.1701% | 1.629 | 4638.396 | 10136.423 | -18.457 | +4617.166 | +10104.431 | 0.698 | - |
| 24000 | proxy_kafka | 3021.3 | 25.6821% | 0.011 | 4036.940 | 10217.341 | -20.076 | +4015.710 | +10185.349 | 0.285 | 1.9774% (0 dropped + 1485 failed of 75097) |
| 32000 | direct | 31968.2 | 100.0000% | 20.095 | 23.665 | 33.447 | - | - | - | - | - |

## Streaming (SSE)

| rate (req/s) | target | achieved (req/s) | ok | p50 | p95 | p99 | added p50 | added p95 | added p99 | overhead (mean) | lost captures |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000 | direct | 997.5 | 100.0000% | 50.643 | 51.183 | 72.753 | - | - | - | - | - |
| 1000 | proxy | 997.4 | 100.0000% | 50.821 | 51.416 | 68.697 | +0.178 | +0.233 | -4.056 | 0.075 | - |
| 1000 | proxy_kafka | 997.4 | 100.0000% | 50.823 | 51.738 | 73.823 | +0.180 | +0.555 | +1.070 | 0.083 | 0.0000% (0 dropped + 0 failed of 20000) |
| 2000 | direct | 1994.9 | 100.0000% | 50.640 | 51.091 | 69.629 | - | - | - | - | - |
| 2000 | proxy | 1994.8 | 100.0000% | 50.834 | 52.085 | 73.718 | +0.194 | +0.993 | +4.089 | 0.088 | - |
| 2000 | proxy_kafka | 1995.0 | 100.0000% | 50.802 | 51.744 | 73.242 | +0.161 | +0.653 | +3.613 | 0.088 | 0.0000% (0 dropped + 0 failed of 40000) |
| 4000 | direct | 3989.6 | 100.0000% | 50.626 | 51.151 | 68.356 | - | - | - | - | - |
| 4000 | proxy | 3989.8 | 100.0000% | 51.255 | 59.130 | 152.828 | +0.629 | +7.979 | +84.472 | 0.200 | - |
| 4000 | proxy_kafka | 3989.9 | 100.0000% | 51.088 | 60.721 | 90.758 | +0.462 | +9.570 | +22.402 | 0.178 | 0.0000% (0 dropped + 0 failed of 80000) |
| 8000 | direct | 7979.8 | 100.0000% | 50.979 | 60.321 | 95.549 | - | - | - | - | - |
| 8000 | proxy | 2077.6 | 36.8108% | 0.177 | 2717.491 | 10000.169 | -50.802 | +2657.170 | +9904.620 | 1.267 | - |
| 8000 | proxy_kafka | 2384.9 | 43.6331% | 0.040 | 2093.786 | 5338.618 | -50.939 | +2033.466 | +5243.069 | 1.400 | 42.9421% (0 dropped + 30199 failed of 70325) |
| 12000 | direct | 6113.7 | 74.0872% | 130.116 | 610.761 | 1093.457 | - | - | - | - | - |

## Maximum sustained throughput

The highest tested rate at which at least 99.9% of requests succeeded and
achieved throughput was at least 99% of the rate asked for. Rates tested:
1000, 2000, 4000, 8000, 12000, 16000, 24000, 32000 req/s. A value equal to the highest tested rate means the
limit was not reached. After a target fails at a rate, higher rates are not
run for it, so it has no rows above that rate. The load generator and the fake upstream share the
machine with the gateway, so the `direct` row is the ceiling of the test
setup itself.

| mode | target | max sustained (req/s) | first rate that failed |
|---|---|---:|---:|
| nonstream | direct | 32000 | none |
| nonstream | proxy | 16000 | 24000 |
| nonstream | proxy_kafka | 16000 | 24000 |
| stream | direct | 8000 | 12000 |
| stream | proxy | 4000 | 8000 |
| stream | proxy_kafka | 4000 | 8000 |

