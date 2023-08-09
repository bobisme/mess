use mess::db::sqlx::migration;
use sqlx::{migrate::MigrateDatabase, Connection, Sqlite, SqliteConnection};
use tracing::info;
use tracing_subscriber::{filter::LevelFilter, fmt, prelude::*, EnvFilter};

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let db_url = std::env::var("DATABASE_URL")?;
    if !Sqlite::database_exists(&db_url).await? {
        info!(db_url, "database doesn't exist, creating");
        Sqlite::create_database(&db_url).await?
    }
    let mut conn = SqliteConnection::connect(&db_url).await.unwrap();
    info!(db_url, "running migrations");
    migration::migrate(&mut conn).await?;
    Ok(())
}
