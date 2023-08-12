pub mod connection;
pub mod migration;
pub mod read;
pub mod write;

#[cfg(test)]
pub(crate) mod test {
    use rusqlite::Connection;

    #[allow(dead_code)]
    pub(crate) fn new_memory_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
        conn.pragma_update(None, "temp_store", "memory").unwrap();
        conn.pragma_update(None, "mmap_size", 10_000_000_000u64).unwrap();
        conn
    }

    #[allow(dead_code)]
    pub(crate) fn new_memory_conn_with_migrations() -> Connection {
        let mut conn = new_memory_conn();
        crate::rusqlite::migration::migrate(&mut conn).unwrap();
        conn
    }
}
