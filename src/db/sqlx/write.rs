use ident::Id;
use serde::Serialize;
use sqlx::types::Json;

use crate::{
    db::Position,
    error::{Error, MessResult},
};

pub async fn write_message(
    executor: impl sqlx::SqliteExecutor<'_>,
    msg_id: Id,
    stream_name: &str,
    msg_type: &str,
    data: impl Serialize + Sync,
    meta: Option<impl Serialize + Sync>,
    expected_version: Option<i64>,
) -> MessResult<Position> {
    let next_position = expected_version.unwrap_or(0);
    let msg_id_str = msg_id.to_string();
    let data = Json(data);
    let meta = meta.map(|x| Json(x));
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
    .fetch_one(executor)
    // .fetch_one(&mut *tx)
    .await
    .map_err(|err| {
        if err.to_string().contains("stream position mismatch") {
            Error::WrongStreamPosition { stream: stream_name.to_owned() }
        } else {
            Error::SqlxError(err)
        }
    })?;
    // tx.commit().await?;
    Ok(Position::new(
        record.global_position as u64,
        Some(next_position.unsigned_abs()),
    ))
}

#[cfg(test)]
mod test {
    use super::*;

    mod write_message_fn {
        use crate::db::sqlx::test::new_memory_pool;

        use super::*;
        use rstest::*;
        use serde_json::json;
        use sqlx::SqlitePool;

        #[fixture]
        async fn test_db() -> SqlitePool {
            let pool = new_memory_pool().await;
            crate::db::sqlx::migration::mig(&pool).await.unwrap();
            pool
        }

        #[rstest]
        async fn it_writes_messages(
            #[future] test_db: SqlitePool,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let mut conn = test_db.await.acquire().await.unwrap();
            let data = json!({ "one": 1, "two": 2 });
            let meta = json!({ "three": 3, "four": 4 });
            let pos = write_message(
                &mut *conn,
                Id::from("fartxx.poopxx"),
                "thing-xyz123.twothr",
                "Donked",
                &data,
                Some(&meta),
                None,
            )
            .await?;
            assert_eq!(pos, Position { global: 1, stream: Some(0) });

            let rows = sqlx::query!("SELECT * FROM messages LIMIT 2")
                .fetch_all(&mut *conn)
                .await?;

            assert_eq!(rows.len(), 1);
            let row = &rows[0];
            assert_eq!(row.global_position, 1);
            assert_eq!(row.position, 0);
            // assert!(row.time, "poot!");
            assert_eq!(row.stream_name, "thing-xyz123.twothr");
            assert_eq!(row.message_type, "Donked");
            assert_eq!(row.data, json!({"one":1,"two":2}).to_string());
            assert_eq!(
                row.metadata,
                Some(json!({"three":3,"four":4}).to_string())
            );
            assert_eq!(row.id, "fartxx.poopxx");
            Ok(())
        }

        #[rstest]
        async fn it_errors_if_stream_version_is_unexpected(
            #[future] test_db: SqlitePool,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let mut conn = test_db.await.acquire().await.unwrap();
            let res = write_message(
                &mut *conn,
                Id::from("fartxx.poopxx"),
                "thing-xyz123.twothr",
                "Donked",
                &json!({ "one": 1, "two": 2 }),
                None::<()>,
                Some(77),
            )
            .await;
            let err = res.unwrap_err();
            if let Error::WrongStreamPosition { stream } = err {
                assert_eq!(stream, "thing-xyz123.twothr");
            } else {
                return Err(err.into());
            }
            Ok(())
        }

        #[rstest]
        async fn it_stores_null_when_metadata_is_none(
            #[future] test_db: SqlitePool,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let mut conn = test_db.await.acquire().await.unwrap();
            write_message(
                &mut *conn,
                Id::new(),
                "stream1",
                "X",
                "data",
                None::<()>,
                None,
            )
            .await?;
            let rec = sqlx::query!("SELECT * FROM messages LIMIT 1")
                .fetch_one(&mut *conn)
                .await?;
            assert_eq!(rec.metadata, None);
            Ok(())
        }

        #[rstest]
        async fn it_stores_json_metadata_when_some(
            #[future] test_db: SqlitePool,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let mut conn = test_db.await.acquire().await.unwrap();
            write_message(
                &mut *conn,
                Id::new(),
                "stream2",
                "X",
                "data",
                Some(&json!({ "some": "meta" })),
                None,
            )
            .await?;
            let rec = sqlx::query!("SELECT * FROM messages LIMIT 1")
                .fetch_one(&mut *conn)
                .await?;
            assert_eq!(
                rec.metadata,
                Some(json!({ "some": "meta" }).to_string())
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod testprops {
    use super::*;
    use proptest::prelude::*;
    use rstest::*;
    use sqlx::SqlitePool;

    async fn test_db() -> SqlitePool {
        crate::db::sqlx::test::new_memory_pool_with_migrations().await
    }

    proptest! {
        #[rstest]
        fn write_message_doesnt_crash(
            msg_type in "\\PC*", stream_name in "\\PC*",
            data in "\\PC*", meta in "\\PC*",
        ) {
            async_std::task::block_on(async {
                let pool = test_db().await;
                let pos = write_message(
                    &pool,
                    Id::new(),
                    &stream_name,
                    &msg_type,
                    &data,
                    Some(&meta),
                    None,
                )
                .await.unwrap();
                assert_eq!(pos, Position { global: 1, stream: Some(0) });
            });
        }
    }
}
