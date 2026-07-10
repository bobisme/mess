//! The bank-account domain, run end-to-end through the mess v1 API.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example bank
//! ```
//!
//! See `src/lib.rs` for the domain (events, aggregate, commands) and
//! `tests/gwt.rs` for the same domain exercised store-free through
//! `mess-testkit`.

use bank::{Account, AccountError, Deposit, Open, Withdraw};
use mess_core::CommandError;
use mess_store::{EventStore, LogEngine, Version};

#[tokio::main]
async fn main() {
    // `EventStore` is the north-star facade: `load` / `append` / `command`
    // over any `Backend`. The default backend is now the composed production
    // engine (`LogEngine`): `mess-log` (durable append log + recovery) +
    // `mess-index` (hot/sealed index + fjall meta tables). The interim
    // in-memory backend is behind mess-store's `mock` feature. Nothing below
    // this line changes with the backend — that is the API-first payoff.
    let dir =
        std::env::temp_dir().join(format!("mess-bank-{}", std::process::id()));
    let store = EventStore::new(LogEngine::open(&dir).expect("open engine"));
    let stream = "account-alice";

    // `command()` is the north-star call: `load -> decide -> append`, with
    // bounded, jittered optimistic retry if another writer races the same
    // stream. Nothing here talks about I/O, retries, or concurrency — that
    // is entirely the store's job, not the caller's.
    store
        .command::<Account, _>(stream, Open { owner: "alice".into() })
        .await
        .expect("open account");
    println!("opened an account for alice on stream {stream:?}");

    store
        .command::<Account, _>(stream, Deposit { amount: 100 })
        .await
        .expect("deposit 100");
    let commit = store
        .command::<Account, _>(stream, Deposit { amount: 50 })
        .await
        .expect("deposit 50");
    println!("deposited 100 then 50; stream now at {:?}", commit.version);

    // A business-rule rejection surfaces as `CommandError::Domain` carrying
    // the aggregate's own `Decide::Rejection` — here, `AccountError` — not
    // a generic string. Callers match on it exhaustively.
    match store.command::<Account, _>(stream, Withdraw { amount: 1_000 }).await
    {
        Err(CommandError::Domain(AccountError::InsufficientFunds {
            balance,
            requested,
        })) => {
            println!(
                "withdrawal of {requested} rejected: balance is only {balance}"
            );
        }
        other => {
            panic!("expected an insufficient-funds rejection, got {other:?}")
        }
    }

    let commit = store
        .command::<Account, _>(stream, Withdraw { amount: 30 })
        .await
        .expect("withdraw 30");
    println!("withdrew 30; stream now at {:?}", commit.version);

    // `load` replays the whole stream through `Aggregate::apply` — the same
    // call `command` makes internally before it decides. There is no
    // ceiling on stream length at the API level: `load` pages through the
    // backend under the hood.
    let loaded = store.load::<Account>(stream).await.expect("load");
    println!(
        "final state: open={} balance={} (replayed {} events, version {:?})",
        loaded.state.open,
        loaded.state.balance,
        loaded.events_replayed,
        loaded.version,
    );
    assert_eq!(loaded.state, Account { open: true, balance: 120 });
    assert_eq!(loaded.version, Version::At(3));
    assert_eq!(loaded.events_replayed, 4);

    println!("bank example OK");
}
