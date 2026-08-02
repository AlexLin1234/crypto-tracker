# Exchange message schemas

Documented from real captured frames, not from memory or documentation. Every
example below is copied from `docs/samples/*.jsonl`, captured 2026-08-02.

Capture sizes: Kraken 500 frames, Binance.US 500 frames, Coinbase 4 frames.

---

## Summary of the differences that matter

| | Kraken v2 | Coinbase Exchange | Binance.US |
| --- | --- | --- | --- |
| Subscribe method | frame (`method`/`params`) | frame (`type`/`channels`) | **URL path** |
| Envelope | `channel` + `type` | `type` | `stream` + `data` |
| Price/qty encoding | **JSON number (float)** | string | string |
| Timestamp | ISO 8601 string | ISO 8601 string | **epoch millis** |
| Book integrity | **CRC32 checksum** | **none** | **update IDs (`U`/`u`)** |
| Trade sequence | `trade_id` | `sequence` | `t` (trade id) |
| Level removal | `qty: 0` | `size: "0.00000000"` | `"0.00000000"` |

The three columns disagree on every single row. That is the case for the
normalization layer in Milestone 1, and it is the most honest illustration of
"messy real-world data" this project has.

---

## Kraken v2 (`wss://ws.kraken.com/v2`)

Public, no auth. Subscribe by frame; one ack per (channel, symbol) pair — two
subscribe requests covering two symbols produced four acks.

### Subscribe ack
```json
{"method": "subscribe", "result": {"channel": "book", "depth": 10, "snapshot": true, "symbol": "BTC/USD"}, "success": true, "time_in": "2026-08-02T05:43:30.758130Z", "time_out": "2026-08-02T05:43:30.758175Z"}
```
`success` is the field the recon probe's error detection keys on.

### Book snapshot and update
Snapshot and update carry **identical keys** — `symbol`, `bids`, `asks`,
`checksum`, `timestamp` — and are distinguished only by the envelope's `type`.

```json
{"channel": "book", "type": "update", "data": [{"symbol": "ETH/USD", "bids": [], "asks": [{"price": 1873.59, "qty": 0.85311371}], "checksum": 2839908831, "timestamp": "2026-08-02T05:43:31.066741Z"}]}
```

Levels are **objects** (`{"price": ..., "qty": ...}`), not pairs.

### Trade
```json
{"channel": "trade", "type": "update", "data": [{"symbol": "BTC/USD", "side": "sell", "price": 63348.4, "qty": 9.417e-05, "ord_type": "limit", "trade_id": 104622323, "timestamp": "2026-08-02T05:43:39.154709Z"}]}
```

### Heartbeat
```json
{"channel": "heartbeat"}
```
No payload, no symbol — a liveness signal only. It cannot be used to detect a
stalled *symbol*, only a stalled connection.

### Quirks

- **Prices arrive as JSON numbers, not strings.** This is the one genuine
  correctness hazard in this feed: `json.loads` turns `63348.4` into a binary
  float, and money in binary floats does not compare or sum exactly. The
  captured data already contains values like `5.1e-05` in scientific notation.
  The connector must parse with `json.loads(raw, parse_float=Decimal)` to avoid
  losing precision *before* the value is ever seen. The other two venues send
  strings and do not have this problem.
- **Integrity is a CRC32 checksum, not a sequence number.** There is no
  sequence field. A dropped update is detected only when the recomputed
  checksum stops matching. That is a strictly weaker signal than a sequence
  number — it tells you the book is wrong, not how many updates you missed —
  but it is a real, verifiable mechanism.
- `depth: 10` was requested and the book is maintained at that depth, so the
  checksum is computed over the top N levels. Getting the checksum's exact
  input format right is a Milestone 2 task and must be validated against these
  captures.

---

## Coinbase Exchange, legacy feed (`wss://ws-feed.exchange.coinbase.com`)

Public, no auth.

### Subscribe ack
```json
{"type": "subscriptions", "channels": [{"name": "level2_50", "product_ids": ["BTC-USD", "ETH-USD"], "account_ids": null}, {"name": "matches", ...}, {"name": "heartbeat", ...}]}
```

**The probe requested `level2_batch` and the server acked `level2_50`.** The
server silently substituted a different channel name. Any connector must read
the ack rather than assume the requested channel is what it got.

### Snapshot
```json
{"type": "snapshot", "product_id": "ETH-USD", "bids": [["1873.49", "2.98818336"], ...], "asks": [["1873.50", "0.15532160"], ...], "time": "..."}
```

The captured ETH-USD snapshot held **6,334 bid levels and 16,725 ask levels**
in a single ~570 KB frame. Despite the `level2_50` channel name, this is
effectively the full book, not 50 levels. Snapshot size is an operational
concern: it dominates bandwidth on reconnect, and a resync storm across
symbols would be expensive.

### L2 update
```json
{"type": "l2update", "product_id": "ETH-USD", "changes": [["buy", "1872.79", "0.00000000"], ["buy", "1872.29", "0.99689463"]], "time": "..."}
```
Levels are `[side, price, size]` triples; `side` is the word `buy`/`sell`, not
`bid`/`ask`. Size `"0.00000000"` removes the level.

### Trade (`matches` channel)
```json
{"type": "last_match", "trade_id": 831829513, "maker_order_id": "...", "taker_order_id": "...", "side": "sell", "size": "0.14237354", "price": "1873.57", "product_id": "ETH-USD", "sequence": 101198200174, "time": "..."}
```

### Quirks

- **The L2 channel carries no sequence number and no checksum.** Verified
  directly against the captured frames: `l2update` keys are exactly
  `["changes", "product_id", "time", "type"]`, and the snapshot adds only
  `product_id`/`time`. There is *no* field on the book channel from which a
  dropped message could be detected.
  The `sequence` field exists only on the `matches`/`last_match` trade channel,
  which is a different stream and does not cover book updates.
  This is the single most consequential finding of Milestone 0 — see
  `DECISIONS.md` D-004.
- The first frame after subscribing is `last_match`, a one-off replay of the
  most recent trade, not a live trade. Counting it as a live trade would
  double-count. It is distinguished from live trades by `type`: live ones
  arrive as `match`, not `last_match`.

---

## Binance.US (`wss://stream.binance.us:9443/stream?streams=...`)

Public, no auth. **Subscription is encoded in the URL**, so there is no
subscribe frame and no ack. Every frame is wrapped:

```json
{"stream": "ethusd@depth", "data": { ... }}
```

### Depth update
```json
{"stream": "ethusd@depth", "data": {"e": "depthUpdate", "E": 1785649433879, "s": "ETHUSD", "U": 3656115049, "u": 3656115107, "b": [["1873.79000000", "0.00000000"], ...], "a": [...]}}
```

Fields are single letters: `E` event time (ms), `s` symbol, `U` first update ID
in the frame, `u` final update ID, `b` bids, `a` asks.

### Trade
```json
{"stream": "ethusd@trade", "data": {"e": "trade", "E": 1785649534887, "s": "ETHUSD", "t": 51376012, "p": "1873.77000000", "q": "0.01330000", "b": 1909997059, "a": 1909997070, "T": 1785649534887, "m": true, "M": true}}
```

`m` is "buyer is market maker" — it encodes aggressor side indirectly:
`m: true` means the buyer was passive, so the trade was a **sell** aggression.
Getting this backwards silently inverts the buy/sell volume imbalance feature
in Milestone 3, and nothing downstream would flag it.

### Quirks

- **Proper sequence numbers.** Gap detection is `U == previous_u + 1`. This was
  verified against the capture: **493 depth updates across two symbols, zero
  discontinuities.** The mechanism works and the check is already proven
  against real data.
- Timestamps are epoch milliseconds, not ISO 8601 — the normalization layer
  must convert.
- Symbols have no separator (`BTCUSD`), unlike Kraken (`BTC/USD`) and Coinbase
  (`BTC-USD`).
