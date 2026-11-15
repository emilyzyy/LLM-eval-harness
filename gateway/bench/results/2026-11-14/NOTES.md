# Notes on the 2026-11-14 run

Written by hand after reading the raw reports. The numbers are in
[RESULTS.md](RESULTS.md).

## What the run shows

- Below the point where the setup saturates, the gateway adds roughly 0.03 to
  0.2 ms at the median (non-streaming: +0.031 to +0.116 ms from 1,000 to
  16,000 req/s; streaming: +0.178 to +0.629 ms from 1,000 to 4,000 req/s).
- Capture to Kafka made no measurable difference to the median: at every
  sustained rate the `proxy_kafka` p50 is within 0.04 ms of the `proxy` p50
  for non-streaming, and within 0.17 ms for streaming.
- No capture records were lost at any sustained rate (0 dropped and 0 failed,
  up to 320,000 requests in one run).
- p99 is noisy. Several "added p99" values are negative, which means the
  direct run happened to have a worse tail than the proxied one. With 20 s
  runs on a laptop that is also running the load generator, differences of a
  few milliseconds at p99 are not reliable in either direction. At 12,000 and
  16,000 req/s the added p99 is consistently positive (+5.7 to +13.0 ms),
  which is more likely real.
- Streaming at 4,000 req/s is close to the edge: p50 is still fine but added
  p95 is about +8 to +10 ms, and one `proxy` run has a p99 of 152.8 ms.

## Why the failed runs failed

The runs that did not sustain their rate (`proxy` and `proxy_kafka` at 24,000
req/s non-streaming and at 8,000 req/s streaming; `direct` at 12,000 req/s
streaming) did not degrade gradually. They collapsed. The errors in the raw
vegeta reports are mostly:

- `socket: too many open files` (the 10,240 open-file limit in `machine.txt`);
- `bind: can't assign requested address` (macOS has 16,384 ephemeral ports,
  49152 to 65535, and a closed connection holds its port for a while);
- 502 from the gateway, when it hit the same limits dialling the fake
  upstream.

So once latency starts to rise, open connections pile up, a per-machine
socket limit is reached, and nearly everything fails. Every proxied request
needs two connections on this one machine (client to gateway, gateway to
upstream), so the gateway paths reach the limit at a lower rate than the
direct path. The direct streaming path reaches it too.

This means the "maximum sustained throughput" figures are the ceiling of this
single-machine setup. They are not a measurement of the gateway's CPU limit,
and the true limit lies somewhere between the last rate that passed and the
first that failed (16,000 to 24,000 req/s non-streaming; 4,000 to 8,000 req/s
streaming). Rates in between were not tested.

## Lost captures in the failed runs

In the two failed `proxy_kafka` runs, Kafka did not acknowledge 1,485 of
75,097 records (2.0%) and 30,199 of 70,325 (42.9%). Nothing was dropped at the
capture queue. The failures were not investigated further; the likely cause is
the same socket exhaustion affecting the Kafka connection, or the client's
10,000-record buffer filling. The gateway's logs were discarded during the
benchmark, so the per-record errors were not kept.

## What was not measured

- Anything on separate machines or over a real network.
- Runs longer than 20 seconds.
- Run-to-run variance: each configuration was run once.
- Behaviour with a slow or failing broker under load (the integration test
  covers a stopped broker at low rate).
