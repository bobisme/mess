-- Add migration script here
CREATE TABLE messages (
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,
    position INTEGER NOT NULL,
    time TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    stream_name TEXT NOT NULL,
    message_type TEXT NOT NULL,
    data TEXT NOT NULL, --JSON
    metadata TEXT, --JSON
    id TEXT NOT NULL UNIQUE
)
STRICT;
