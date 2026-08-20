# Benchmarks

All figures measured on the machine described below, against the real captured
fixtures. Reproduce with:

```bash
uv run python -m xstream.bench --brokers localhost:9092      # per-stage
uv run python -m xstream.bench.loadtest --brokers localhost:9092
uv run python -m xstream.bench.consumer_lag --brokers localhost:9092
```

**Environment:** 4 vCPU, 15 GB RAM, Ubuntu 24.04, Python 3.11, OpenJDK 21,
single-node Apache Kafka 4.1.2 (KRaft) on the same host. Single process, single
core for the CPU stages. A single-node broker on the same machine as its client
has no network hop, so broker figures here are an upper bound on what a real
deployment would see.

---

## Headline

| | |
| --- | --- |
| **Bottleneck** | Order book reconstruction with checksum verification, **~34,000 deltas/s** |
| Producer ceiling | 579,260 msg/s |
| Consumer drain | 44,045 msg/s |
| Cost of correctness (Decimal) | **1.62x** slower parsing |
| Cost of correctness (checksum) | **8.4x** slower book updates |
| Latency percentiles | **Not measured — see below** |

The pipeline is CPU-bound in the stateful stage, not I/O-bound at the broker.
The broker is roughly 17x faster than the slowest CPU stage, so adding brokers
would buy nothing; the way to scale this is to shard books across processes by
(exchange, symbol), which the symbol-keyed partitioning already permits.

---

## Per-stage throughput

Best of 5 runs over the 500-frame Kraken fixture, pre-parsed into memory so
disk I/O does not contaminate the measurement. Each stage reports the unit it
actually consumes — comparing "messages/sec" across stages that mean different
things by "message" is the easiest way to misread a table like this.

| stage | unit | per second | µs each | note |
| --- | --- | ---: | ---: | --- |
| json parse (float) | frames | 330,330 | 3.03 | stdlib; loses price precision |
| json parse (Decimal) | frames | 203,851 | 4.91 | required for correctness (D-003) |
| normalize | messages out | 237,461 | 4.21 | 500 frames in |
| encode (JSON bytes) | messages | 126,657 | 7.90 | Decimal → string |
| **book apply + checksum** | **deltas** | **33,916** | **29.48** | **bottleneck** |
| book apply (no checksum) | deltas | 286,232 | 3.49 | same path, verification off |
| produce to broker | messages | 579,260 | 1.73 | sustained, unthrottled |
| consumer drain | messages | 44,045 | 22.70 | 171,000 messages in 3.88 s |

### What correctness costs

Two decisions made earlier for correctness were promised measurements rather
than assurances. Here they are.

**Decimal parsing costs 1.62x** (330,330 → 203,851 frames/s). D-003 chose
`parse_float=Decimal` because an order book keys on exact price equality and a
float one ULP off is a different level. 1.62x on a stage running at 200k/s is
not close to being the constraint, so the trade was cheap and is worth keeping.

**Checksum verification costs 8.4x** (286,232 → 33,916 deltas/s; 3.49 µs →
29.48 µs). This is the entire bottleneck: verification is 88% of the time spent
in the stateful stage. Recomputing a CRC32 over the top 10 levels on *every*
update is genuinely expensive relative to applying the update itself.

It stays on. An unverified book is the failure mode D-013 exists to prevent —
silent corruption feeding plausible numbers to everything downstream — and
34k deltas/s is still roughly 1,500x the fixture's real-time rate of 23 msg/s.
If it ever became the real constraint, the honest lever is verifying every Nth
update and accepting a bounded detection delay, not removing verification.

---

## Load test

The fixture spans 412 s of market time at 23 msg/s. Replaying at Nx compresses
that span into `span / N` seconds.

| speed | target | achieved | backpressure | verdict |
| --- | ---: | ---: | ---: | --- |
| 10x | 231/s | 231/s | 0 | kept up |
| 100x | 2,305/s | 2,305/s | 0 | kept up |
| unthrottled (9.5k msgs) | — | 411,827/s | 0 | — |
| unthrottled (95k msgs) | — | 579,260/s | 0 | — |

10x and 100x are not interesting: they are three orders of magnitude below the
ceiling, and reporting "it kept up" would be the least informative possible
benchmark. The unthrottled runs are the ones that locate the actual limit.

---

## Induced failure modes

### Backpressure

At the default 20,000-message producer queue, **backpressure never occurs** —
librdkafka's background thread drains faster than a single Python thread can
enqueue. That is itself the finding at normal settings, and it means the
`BufferError` path in the producer was never being exercised in practice.

Shrinking the queue to 100 forces it:

| queue depth | messages | throughput | BufferError events |
| ---: | ---: | ---: | ---: |
| 20,000 | 95,000 | 579,260/s | 0 |
| 100 | 47,500 | **926/s** | **474** |

**A 625x throughput collapse.** Each `BufferError` blocks the producing thread
in `poll()` until the queue drains, so an undersized queue converts a fast
pipeline into a slow one without dropping a single message.

That is the correct trade — the alternative is silently discarding market data
— but it means queue depth is a latency/throughput tuning knob with a very
sharp edge, and the symptom (throughput collapse, no errors logged) looks
nothing like the cause.

### Consumer lag

Producing 171,000 messages and then draining from `earliest`:

- drained **171,000 messages in 3.88 s** → 44,045 msg/s
- **remaining lag: 0** — the consumer caught up fully

Measuring drain time rather than using a fixed window matters here: a 10 s
window that included idle time after catch-up reported 16,419/s, understating
the true rate by 2.7x. A benchmark that keeps counting after the work is done
measures the window, not the system.

### Partition skew

Measured over the 171,000-message benchmark topic with 6 partitions:

| partition | messages | share |
| ---: | ---: | ---: |
| 2 | 46,080 | 26.9% |
| 3 | 124,920 | 73.1% |
| 0, 1, 4, 5 | 0 | 0% |

**Two of six partitions carry all the data, and one holds 73.1% of it.**

This is designed-in, not accidental. D-008 keys messages by canonical symbol so
that per-venue ordering is preserved and the Milestone 4 cross-exchange join
gets partition locality. With two symbols, only two partitions can ever receive
data, and the split follows real message volume rather than anything balanced.

The consequences are concrete: Spark launches 6 tasks of which 4 read nothing,
and the busiest task does **2.71x** the work of the other. At this scale the
waste is irrelevant. At production scale the fix is more symbols (which spreads
keys naturally) or a composite `(exchange, symbol)` key — which D-008 rejected
because it splits the two sides of the divergence join across partitions,
trading a join shuffle for parallelism the project does not yet need.

---

## Latency: deliberately not reported

`BENCHMARKS.md` has no p50/p95/p99 latency table, and the omission is the
honest result rather than an oversight.

Latency is computed as venue timestamp minus ingest timestamp. On a live feed
that is genuine end-to-end latency. On **replayed fixtures it is the age of the
capture** — the data was captured on 2026-08-02 and replayed on 2026-08-11, so
the pipeline dutifully reported a p50 of roughly 808,500,000 ms, about nine
days. Arithmetically correct, analytically worthless, and dangerous precisely
because it looks like a measurement.

`queries/03_ingest_latency_percentiles.sql` now emits a `measurement_valid`
column that reads `NO - replayed fixture, not live ingest` whenever the maximum
exceeds a minute. Publishing a latency figure from this data would be measuring
the wrong thing, so none is published.

**Throughput benchmarks from replay are valid** — replay is the right tool for
load testing, and nothing above depends on wall-clock alignment with market
time. Only the latency column needs a live connection, which this environment
cannot open (D-000).

---

## What would break first at scale

Ranked by what the measurements actually show, not by intuition:

1. **Book reconstruction, at ~34k deltas/s per process.** The only stage within
   an order of magnitude of being a real constraint. It is single-threaded and
   sequential per (exchange, symbol), so the scaling answer is sharding by that
   key across processes rather than optimizing the inner loop.
2. **Consumer drain, at ~44k msg/s per consumer.** Kafka's answer is more
   consumers in the group — which the current partitioning caps at 2 useful
   ones, since 4 partitions are empty. Partition skew becomes a live constraint
   here before it becomes one anywhere else.
3. **Producer queue depth.** Not a throughput limit at default settings, but a
   625x cliff if it is ever undersized relative to burst rate.
4. **The broker itself is not close.** 579k msg/s against a 34k/s pipeline
   means the infrastructure has roughly 17x headroom over the code.

The fixture is 40 seconds of quiet market data, so none of these has been
exercised at a scale where memory pressure, GC, or disk throughput would enter
the picture. Those need a long capture, which is a known gap (D-014).
