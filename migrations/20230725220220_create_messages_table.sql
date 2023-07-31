-- PRAGMA journal_mode = WAL;
-- PRAGMA synchronous = normal;
-- PRAGMA temp_store = memory;
-- PRAGMA mmap_size = 30000000000;

CREATE TABLE messages (
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,
    position INTEGER NOT NULL,
    time TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    stream_name TEXT NOT NULL,
    message_type TEXT NOT NULL,
    data TEXT NOT NULL, --JSON
    metadata TEXT, --JSON
    id TEXT NOT NULL UNIQUE,
    -- Virtual columns
    category TEXT AS (substring(stream_name, 1, instr(stream_name, '-') - 1)) VIRTUAL,
    stream_id TEXT AS (substring(stream_name, instr(stream_name, '-') + 1)) VIRTUAL,
    cardinal_id TEXT AS (substring(stream_id, 1, instr(stream_id, '+') - 1)) VIRTUAL
)
STRICT;

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