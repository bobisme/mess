# codec bake-off

corpus: 10000 test + 5000 train events per type x 3 types; zstd level 3, 16 KiB dictionaries; blocks of 128 events

## Table 1 — mean encoded size (bytes/event, uncompressed)

| codec | OrderPlaced | UserRegistered | ShipmentEvent | overall |
|---|---|---|---|---|
| json | 640.1 | 281.7 | 147.2 | 356.3 |
| cbor | 508.8 | 224.0 | 110.7 | 281.2 |
| msgpack-named | 508.7 | 222.3 | 110.6 | 280.5 |
| msgpack-compact | 188.8 | 94.0 | 58.6 | 113.8 |
| postcard | 168.7 | 74.0 | 40.0 | 94.2 |
| bincode | 298.0 | 113.8 | 65.3 | 159.0 |

## Table 2 — compressed size (bytes/event, overall mean)

| codec | raw | zstd-3 solo | zstd-3 +16KiB dict | 128-ev block | 128-ev block +dict | dict vs best-raw* |
|---|---|---|---|---|---|---|
| json | 356.3 | 246.4 | 75.1 | 57.7 | 53.1 | -20% |
| cbor | 281.2 | 225.1 | 72.8 | 56.3 | 51.4 | -23% |
| msgpack-named | 280.5 | 233.0 | 72.6 | 56.1 | 51.1 | -23% |
| msgpack-compact | 113.8 | 119.6 | 64.9 | 48.2 | 44.0 | -31% |
| postcard | 94.2 | 100.6 | 60.3 | 45.2 | 40.6 | -36% |
| bincode | 159.0 | 130.5 | 68.7 | 51.8 | 47.8 | -27% |

*dict vs best-raw: per-event dictionary-compressed size relative to the smallest UNcompressed codec (postcard/bincode class).

## Table 3 — throughput (30k events, best of 5 reps)

| codec | encode Mev/s | encode MB/s | decode Mev/s | decode MB/s | enc+dictzstd Mev/s | dictzstd+dec Mev/s |
|---|---|---|---|---|---|---|
| json | 1.69 | 604 | 0.91 | 323 | 0.47 | 0.63 |
| cbor | 1.91 | 538 | 0.62 | 175 | 0.53 | 0.49 |
| msgpack-named | 1.82 | 511 | 1.39 | 389 | 0.53 | 0.88 |
| msgpack-compact | 2.79 | 317 | 2.71 | 309 | 0.67 | 1.48 |
| postcard | 3.52 | 332 | 4.05 | 382 | 0.69 | 1.99 |
| bincode | 14.57 | 2317 | 4.95 | 787 | 0.81 | 1.78 |

## Appendix — per-type dictionary compression (bytes/event)

| codec | type | raw | +dict | ratio |
|---|---|---|---|---|
| json | OrderPlaced | 640.1 | 110.2 | 5.81x |
| json | UserRegistered | 281.7 | 63.3 | 4.45x |
| json | ShipmentEvent | 147.2 | 51.7 | 2.85x |
| cbor | OrderPlaced | 508.8 | 111.5 | 4.56x |
| cbor | UserRegistered | 224.0 | 58.9 | 3.80x |
| cbor | ShipmentEvent | 110.7 | 48.1 | 2.30x |
| msgpack-named | OrderPlaced | 508.7 | 111.7 | 4.55x |
| msgpack-named | UserRegistered | 222.3 | 58.1 | 3.82x |
| msgpack-named | ShipmentEvent | 110.6 | 48.0 | 2.30x |
| msgpack-compact | OrderPlaced | 188.8 | 97.9 | 1.93x |
| msgpack-compact | UserRegistered | 94.0 | 51.6 | 1.82x |
| msgpack-compact | ShipmentEvent | 58.6 | 45.3 | 1.29x |
| postcard | OrderPlaced | 168.7 | 91.1 | 1.85x |
| postcard | UserRegistered | 74.0 | 46.3 | 1.60x |
| postcard | ShipmentEvent | 40.0 | 43.6 | 0.92x |
| bincode | OrderPlaced | 298.0 | 106.0 | 2.81x |
| bincode | UserRegistered | 113.8 | 51.2 | 2.22x |
| bincode | ShipmentEvent | 65.3 | 49.0 | 1.33x |

## Evolution matrix (encode V1, decode V2)

| schema change | json | cbor | msgpack-named | msgpack-compact | postcard | bincode |
|---|---|---|---|---|---|---|
| (a) added Option field | OK | OK | OK | ERROR | ERROR | ERROR |
| (b) added field w/ serde default | OK | OK | OK | OK | ERROR | ERROR |
| (c) removed field | OK | OK | OK | ERROR | ERROR | ERROR |
| (d) renamed field (no serde rename) | ERROR | ERROR | ERROR | OK | OK | OK |
| (e1) reordered fields (same-typed adjacent swap) | OK | OK | OK | SILENT-WRONG | SILENT-WRONG | SILENT-WRONG |
| (e2) reordered fields (mixed types moved) | OK | OK | OK | ERROR | ERROR | ERROR |
| (f) enum: variant added at end | OK | OK | OK | OK | OK | OK |
| (g) enum: variants reordered | OK | OK | OK | OK | SILENT-WRONG | SILENT-WRONG |
| (h) int widened u32 -> u64 | OK | OK | OK | OK | OK | ERROR |

### SILENT-WRONG details

- `msgpack-compact` / (e1) reordered fields (same-typed adjacent swap): got V2DimsSwapped { id: 7, name: "alice", height: 1920, width: 1080, active: true }, expected V2DimsSwapped { id: 7, name: "alice", height: 1080, width: 1920, active: true }
- `postcard` / (e1) reordered fields (same-typed adjacent swap): got V2DimsSwapped { id: 7, name: "alice", height: 1920, width: 1080, active: true }, expected V2DimsSwapped { id: 7, name: "alice", height: 1080, width: 1920, active: true }
- `bincode` / (e1) reordered fields (same-typed adjacent swap): got V2DimsSwapped { id: 7, name: "alice", height: 1920, width: 1080, active: true }, expected V2DimsSwapped { id: 7, name: "alice", height: 1080, width: 1920, active: true }
- `postcard` / (g) enum: variants reordered: got Deleted(9), expected Created(9)
- `bincode` / (g) enum: variants reordered: got Deleted(9), expected Created(9)

### ERROR details (loud failures, acceptable)

- `msgpack-compact` / (a) added Option field: invalid length 4, expected struct V2AddedOption with 5 elements
- `postcard` / (a) added Option field: Hit the end of buffer, expected more data
- `bincode` / (a) added Option field: io error: unexpected end of file
- `postcard` / (b) added field w/ serde default: Hit the end of buffer, expected more data
- `bincode` / (b) added field w/ serde default: io error: unexpected end of file
- `msgpack-compact` / (c) removed field: invalid type: integer `42`, expected a boolean
- `postcard` / (c) removed field: Found a bool that wasn't 0 or 1
- `bincode` / (c) removed field: invalid u8 while decoding bool, expected 0 or 1, found 42
- `json` / (d) renamed field (no serde rename): missing field `title` at line 1 column 48
- `cbor` / (d) renamed field (no serde rename): Semantic(None, "missing field `title`")
- `msgpack-named` / (d) renamed field (no serde rename): missing field `title`
- `msgpack-compact` / (e2) reordered fields (mixed types moved): wrong msgpack marker FixStr(5)
- `postcard` / (e2) reordered fields (mixed types moved): Hit the end of buffer, expected more data
- `bincode` / (e2) reordered fields (mixed types moved): io error: 
- `bincode` / (h) int widened u32 -> u64: io error: unexpected end of file
