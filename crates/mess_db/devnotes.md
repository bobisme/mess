# Backends
## sqlx using sqlite

Worked. Was pretty quick. The convenience of compile-time type checking queries was a blessing and a curse.

## rusqlite

Not as convenient as sqlx, but since the data model is super simple it doesn't matter.
Twice as fast as sqlx.

## rocksdb

Had been thinking about this for years since reads involve a lot of scanning.
10x as fast as rusqlites in writes.

Decided to remove sqlx entirely.
