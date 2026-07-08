-- PRAGMA journal_mode = WAL;
-- PRAGMA synchronous = normal;
-- PRAGMA temp_store = memory;
-- PRAGMA mmap_size = 30000000000;

DROP TABLE IF EXISTS messages;
CREATE TABLE messages (
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,
    position INTEGER NOT NULL,
    time_ms INTEGER NOT NULL DEFAULT (
      CAST(
        (CASE WHEN sqlite_version() >= '3.42.0' THEN
          unixepoch('subsec')
        ELSE
          unixepoch()
        END) * 1000 AS integer)
      ),
    stream_name TEXT NOT NULL,
    message_type TEXT NOT NULL,
    data TEXT NOT NULL, --JSON
    metadata TEXT, --JSON
    id TEXT NOT NULL UNIQUE,
    ord INTEGER DEFAULT (-(
      ((
        CAST((
          CASE WHEN sqlite_version() >= '3.42.0' THEN
            unixepoch('subsec') - unixepoch('2020-01-01', 'subsec')
          ELSE
            unixepoch() - unixepoch('2020-01-01')
          END
        ) * 20 as integer)
      ) << 16)
      -- + (0 << 56) -- era
    )),
    -- Virtual columns
    ord_unix REAL AS 
      (cast(ord >> 16 as float) / 20 + unixepoch('2020-01-01', 'subsec')) VIRTUAL,
    ord_time TEXT AS (datetime(ord_unix, 'unixepoch', 'subsec')) VIRTUAL,
    category TEXT AS (substring(stream_name, 1, instr(stream_name, '-') - 1)) VIRTUAL,
    stream_id TEXT AS (substring(stream_name, instr(stream_name, '-') + 1)) VIRTUAL,
    cardinal_id TEXT AS (substring(stream_id, 1, instr(stream_id, '+') - 1)) VIRTUAL
)
STRICT;

-- INDEX: messages.ord
DROP INDEX IF EXISTS messages_ord;
CREATE INDEX messages_ord ON messages (ord);

-- DROP VIEW IF EXISTS clock_timestamp;
-- CREATE VIEW clock_timestamp (ts) AS
-- SELECT (
--   ((
--     cast(unixepoch('subsec') * 20 as integer)
--     - cast(unixepoch('2020-01-01', 'subsec') * 20 as integer)
--   ) << 16)
--   + (0 << 56) -- era
-- );

-- new ord
DROP TRIGGER IF EXISTS clock_timestamp;
CREATE TRIGGER clock_timestamp
AFTER INSERT ON messages
FOR EACH ROW
BEGIN
    UPDATE messages 
    SET ord = MAX(-NEW.ord, (SELECT MAX(ord) + 1 FROM messages))
    WHERE global_position = NEW.global_position AND NEW.ord < 0;
END;

-- CHECK: messages.position must match the next sequential
CREATE TRIGGER check_stream_position
BEFORE INSERT ON messages
FOR EACH ROW
BEGIN
    SELECT CASE WHEN 
        IFNULL((
            SELECT position
            FROM messages
            WHERE stream_name = NEW.stream_name
            ORDER BY global_position DESC
            LIMIT 1
        ), -1) != NEW.position - 1 
    THEN RAISE(ROLLBACK, 'stream position mismatch') END;
END;

-- INDEX: messages.id
DROP INDEX IF EXISTS messages_id;
CREATE UNIQUE INDEX messages_id ON messages (id);

-- INDEX: messages.category
DROP INDEX IF EXISTS messages_category;
CREATE INDEX messages_category ON messages (
    category,
    global_position,
    substr(metadata->>'correlationStreamName', 1, instr(metadata->>'correlationStreamName', '-') - 1)
);
