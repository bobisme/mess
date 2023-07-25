use ident::Id;
use serde::Serialize;
use sqlx::{types::Json, Connection, SqliteConnection};

use crate::{
    db::Position,
    error::{Error, MessResult},
};

async fn stream_version(
    conn: &mut SqliteConnection,
    stream_name: &str,
) -> MessResult<Option<i64>> {
    let record = sqlx::query!(
        r#"
        SELECT
            max(position) as pos
        FROM
            messages
        WHERE
            stream_name = $1;
        "#,
        stream_name,
    )
    .fetch_one(conn)
    .await?;
    Ok(record.pos)
}

async fn write_message(
    conn: &mut SqliteConnection,
    msg_id: Id,
    stream_name: &str,
    msg_type: &str,
    data: impl Serialize + Sync,
    meta: impl Serialize + Sync,
    expected_version: Option<i64>,
) -> MessResult<Position> {
    let mut tx = conn.begin().await?;
    let stream_version =
        stream_version(&mut tx, stream_name).await?.unwrap_or(-1);
    if let Some(expected_version) = expected_version {
        if expected_version != stream_version {
            return Err(Error::WrongStreamVersion {
                stream: stream_name.into(),
                expected: expected_version,
                found: stream_version,
            });
        }
    }
    let next_position = stream_version + 1;
    let msg_id_str = msg_id.to_string();
    let data = Json(data);
    let meta = Json(meta);
    let record = sqlx::query!(
        r#"
        INSERT INTO messages (
            id,
            stream_name,
            position,
            message_type,
            data,
            metadata
        ) VALUES (
            $1, $2, $3, $4, $5, $6
        )
        RETURNING global_position;"#,
        msg_id_str,
        stream_name,
        next_position,
        msg_type,
        data,
        meta,
    )
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Position::new(
        record.global_position as u64,
        Some(next_position.unsigned_abs()),
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::*;
    use serde_json::json;

    #[fixture]
    async fn test_db() -> SqliteConnection {
        let mut c = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        crate::db::sqlite::migration::mig(&mut c).await.unwrap();
        c
    }

    #[rstest]
    async fn it_writes_messages(
        #[future] test_db: SqliteConnection,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = test_db.await;
        let data = json!({ "one": 1, "two": 2 });
        let meta = json!({ "three": 3, "four": 4 });
        let pos = write_message(
            &mut conn,
            Id::from("fartxx.poopxx"),
            "thing-xyz123.twothr",
            "Donked",
            &data,
            &meta,
            None,
        )
        .await
        .unwrap();
        assert_eq!(pos, Position { global: 1, stream: Some(0) });

        let rows = sqlx::query!("SELECT * FROM messages LIMIT 2")
            .fetch_all(&mut conn)
            .await
            .unwrap();

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.global_position, 1);
        assert_eq!(row.position, 0);
        // assert!(row.time, "poot!");
        assert_eq!(row.stream_name, "thing-xyz123.twothr");
        assert_eq!(row.message_type, "Donked");
        assert_eq!(row.data, json!({"one":1,"two":2}).to_string());
        assert_eq!(row.metadata, Some(json!({"three":3,"four":4}).to_string()));
        assert_eq!(row.id, "fartxx.poopxx");
        Ok(())
    }
}
