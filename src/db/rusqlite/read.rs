use crate::db::{read::ReadMessages, Message};

// ```
// use mess::db::read::ReadMessages;
// use mess::db::sqlite::read::fetch;
// let read_messages = ReadMessages::default()
//     .from_stream("some_stream_name")
//     .with_limit(5);
// fetch(read_messages, &pool).unwrap();
// ```
pub async fn fetch(
    rm: ReadMessages<'_>,
    executor: impl sqlx::SqliteExecutor<'_>,
) -> Result<Vec<Message>, sqlx::Error> {
    if let Some(stream) = rm.stream_name() {
        get_stream_messages(executor, stream, Some(rm.limit() as i32)).await
    } else {
        get_messages(
            executor,
            rm.global_position() as i32,
            Some(rm.limit() as i32),
        )
        .await
    }
}

pub async fn get_messages(
    executor: impl sqlx::SqliteExecutor<'_>,
    global_position: i32,
    limit: Option<i32>,
) -> Result<Vec<Message>, sqlx::Error> {
    let limit = limit.unwrap_or(1_000).clamp(1, 10_000);
    let messages = sqlx::query_as!(
        Message,
        r#"
        SELECT
            global_position,
            position,
            time_ms,
            stream_name,
            message_type,
            data,
            metadata,
            id
        FROM messages
        WHERE global_position >= $1
        ORDER BY global_position ASC
        LIMIT $2"#,
        global_position,
        limit,
    )
    .fetch_all(executor)
    .await?;
    Ok(messages)
}

pub async fn get_stream_messages(
    executor: impl sqlx::SqliteExecutor<'_>,
    stream_name: &str,
    limit: Option<i32>,
) -> Result<Vec<Message>, sqlx::Error> {
    let limit = limit.unwrap_or(1_000).clamp(1, 10_000);
    let messages = sqlx::query_as!(
        Message,
        r#"
        SELECT
            global_position,
            position,
            time_ms,
            stream_name,
            message_type,
            data,
            metadata,
            id
        FROM messages
        WHERE stream_name = $1
        ORDER BY global_position ASC
        LIMIT $2"#,
        stream_name,
        limit,
    )
    .fetch_all(executor)
    .await?;

    Ok(messages)
}

pub async fn get_latest_stream_message(
    executor: impl sqlx::SqliteExecutor<'_>,
    stream_name: &str,
) -> Result<Option<Message>, sqlx::Error> {
    let message = sqlx::query_as!(
        Message,
        r#"
        SELECT 
            global_position,
            position,
            time_ms,
            stream_name,
            message_type,
            data,
            metadata,
            id
        FROM messages
        WHERE stream_name = $1
        ORDER BY position DESC
        LIMIT 1"#,
        stream_name,
    )
    .fetch_optional(executor)
    .await?;

    Ok(message)
}

pub async fn get_latest_stream_position(
    executor: impl sqlx::SqliteExecutor<'_>,
    stream_name: &str,
) -> Result<Option<i64>, sqlx::Error> {
    let position = sqlx::query_scalar!(
        r#"
        SELECT position
        FROM messages
        WHERE stream_name = $1
        ORDER BY global_position DESC
        LIMIT 1
        "#,
        stream_name,
    )
    .fetch_optional(executor)
    .await?;

    Ok(position)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::sqlx::test::new_memory_pool;
    use rstest::*;
    use sqlx::{QueryBuilder, SqlitePool};

    const ROWS_PER_INSERT: usize = 500;

    async fn test_db(rows_per_stream: i64) -> SqlitePool {
        let rows_per_stream = rows_per_stream.max(0) as usize;
        let pool = new_memory_pool().await;
        crate::db::sqlx::migration::mig(&pool).await.unwrap();

        let rows = (0..).map(|i| {
            [
                (format!("{:x<6}.xxxxxx", i), "stream1", i, "X", i, None::<()>),
                (format!("{:x<6}.xxxxxy", i), "stream2", i, "X", i, None::<()>),
            ]
        });

        for i in (0..rows_per_stream).step_by(ROWS_PER_INSERT) {
            let row_count = (rows_per_stream - i).min(ROWS_PER_INSERT);
            let mut query_builder: QueryBuilder<sqlx::Sqlite> =
                QueryBuilder::new(
                    r#"
            INSERT INTO messages (
                id,
                stream_name,
                position,
                message_type,
                data
            ) 
            "#,
                );
            query_builder.push_values(
                rows.clone().skip(i).take(row_count).flatten(),
                |mut b, row| {
                    b.push_bind(row.0)
                        .push_bind(row.1)
                        .push_bind(row.2)
                        .push_bind(row.3)
                        .push_bind(row.4);
                },
            );
            let query = query_builder.build();
            query.execute(&pool).await.unwrap();
        }
        pool
    }

    mod fn_get_messages {
        use super::*;
        use pretty_assertions::assert_eq;

        #[rstest]
        async fn it_gets_messages_up_to_limit() {
            let pool = test_db(5).await;
            let messages = get_messages(&pool, 0, Some(5)).await.unwrap();
            assert_eq!(messages.len(), 5);
            let m = &messages[0];
            assert_eq!(m.global_position, 1);
            assert_eq!(m.position, 0);
            assert_ne!(m.time_ms, 0);
            assert_eq!(m.stream_name, "stream1");
            assert_eq!(m.message_type, "X");
            assert_eq!(m.data, "0");
            assert_eq!(m.metadata, None);
            assert_eq!(m.id, "0xxxxx.xxxxxx");
        }

        #[rstest]
        async fn it_gets_messages_starting_from_given_pos() {
            let pool = test_db(5).await;
            let messages = get_messages(&pool, 5, Some(2)).await.unwrap();
            assert_eq!(messages.len(), 2);
            let m = &messages[0];
            assert_eq!(m.global_position, 5);
            assert_eq!(m.position, 2);
            assert_ne!(m.time_ms, 0);
            assert_eq!(m.stream_name, "stream1");
            assert_eq!(m.message_type, "X");
            assert_eq!(m.data, "2");
            assert_eq!(m.metadata, None);
            assert_eq!(m.id, "2xxxxx.xxxxxx");
        }

        #[rstest]
        async fn it_returns_empty_vec_if_pos_too_high() {
            let pool = test_db(5).await;
            let messages = get_messages(&pool, 500, Some(10)).await.unwrap();
            assert_eq!(messages.len(), 0);
        }

        #[rstest]
        async fn the_lowest_limit_is_1() {
            let pool = test_db(5).await;
            let messages = get_messages(&pool, 0, Some(-200)).await.unwrap();
            assert_eq!(messages.len(), 1);
        }

        #[rstest]
        async fn the_default_is_1_000() {
            let pool = test_db(550).await;
            let messages = get_messages(&pool, 0, None).await.unwrap();
            assert_eq!(messages.len(), 1_000);
        }

        #[rstest]
        async fn the_max_is_10_000() {
            let pool = test_db(5_010).await;
            let messages = get_messages(&pool, 0, Some(100_000)).await.unwrap();
            assert_eq!(messages.len(), 10_000);
        }
    }

    mod fn_get_stream_messages {
        use super::*;
        use pretty_assertions::assert_eq;
        #[rstest]
        async fn it_only_returns_messages_from_given_stream() {
            let pool = test_db(5).await;
            let messages =
                get_stream_messages(&pool, "stream1", Some(5)).await.unwrap();
            assert_eq!(messages.len(), 5);
            for message in messages {
                assert_eq!(message.stream_name, "stream1");
            }
        }
    }

    mod fn_get_latest_stream_message {
        use super::*;
        use pretty_assertions::assert_eq;

        #[rstest]
        async fn it_returns_messages_with_highest_stream_pos() {
            let pool = test_db(5).await;
            let m = get_latest_stream_message(&pool, "stream1")
                .await
                .unwrap()
                .unwrap();
            assert_ne!(m.time_ms, 0);
            assert_eq!(m.global_position, 9);
            assert_eq!(m.position, 4);
            assert_eq!(m.stream_name, "stream1");
            assert_eq!(m.message_type, "X");
            assert_eq!(m.data, "4");
            assert_eq!(m.metadata, None);
            assert_eq!(m.id, "4xxxxx.xxxxxx");
        }

        #[rstest]
        async fn it_returns_none_if_no_stream() {
            let pool = test_db(5).await;
            let message =
                get_latest_stream_message(&pool, "no-stream").await.unwrap();
            assert_eq!(message, None);
        }
    }

    mod fn_get_latest_stream_position {
        use super::*;
        use pretty_assertions::assert_eq;
        #[rstest]
        async fn it_returns_last_position_for_stream() {
            let pool = test_db(5).await;
            let position =
                get_latest_stream_position(&pool, "stream1").await.unwrap();
            assert_eq!(position, Some(4));
        }

        #[rstest]
        async fn it_returns_none_if_no_stream() {
            let pool = test_db(5).await;
            let position =
                get_latest_stream_position(&pool, "null-stream").await.unwrap();
            assert_eq!(position, None);
        }
    }
}
