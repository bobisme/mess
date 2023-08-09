pub mod migration;
pub mod read;
pub mod write;

#[cfg(test)]
pub(crate) mod test {
    use std::str::FromStr;

    use sqlx::{
        sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
        SqlitePool,
    };

    #[allow(dead_code)]
    pub(crate) async fn new_memory_pool() -> SqlitePool {
        let options = SqliteConnectOptions::from_str("sqlite::memory:")
            .unwrap()
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .pragma("temp_store", "memory")
            .pragma("mmap_size", format!("{}", 10_000_000_000u64));
        SqlitePool::connect_with(options).await.unwrap()
    }

    #[allow(dead_code)]
    pub(crate) async fn new_memory_pool_with_migrations() -> SqlitePool {
        let pool = new_memory_pool().await;
        crate::db::sqlx::migration::mig(&pool).await.unwrap();
        pool
    }
}
