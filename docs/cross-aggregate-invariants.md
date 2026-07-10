# Cross-aggregate invariants: accept-and-reconcile, or saga

Status: **guide** (non-normative). This is the blessed posture for a
recurring application-modeling question that isn't a store protocol
question: what does a `Decide` impl do when a business rule depends on
*another* aggregate's state? Derived from `examples/social`'s `Follow`
command, which is the worked reference throughout (§4). Promoted to a docs
page by bn-2v0.

This document is about **application modeling on top of `mess-core` /
`mess-store`**, not the log/store protocol — that's why it lives here rather
than in `docs/spec/`, which is reserved for normative backend specs
(`01-log-format.md` through `07-backup.md`). See `docs/adr/` for recorded
backend decisions and `docs/verification.md` for another example of a
non-numbered, non-normative docs page.

## 1. Why cross-aggregate preconditions can't be transactional

`mess-core`'s two central traits are deliberately narrow:

```rust
pub trait Aggregate: Default + Send + Sync + 'static {
    type Event: Event;
    fn apply(&mut self, event: &Self::Event);
}

pub trait Decide<C>: Aggregate {
    type Rejection: std::error::Error;
    fn decide(&self, cmd: C) -> Result<Vec<Self::Event>, Self::Rejection>;
}
```

`decide` takes `&self` — the state folded from **one** stream — and nothing
else. There is no store handle, no way to reach out and load a second
stream mid-decision. That's not an oversight; it's what makes
`EventStore::command` (`crates/mess-store/src/store.rs`) work at all:

```
loop {
    let loaded = self.load::<A>(stream_id).await?;       // fold ONE stream
    let events = loaded.state.decide(cmd.clone())?;       // pure fn of that state
    match self.backend.append_batch(stream_id, loaded.version, &records).await {
        Ok(appended) => return Ok(appended),
        Err(AppendError::Conflict { .. }) => { /* reload and retry */ }
        ...
    }
}
```

The optimistic-concurrency check — "is the stream still at the version I
loaded it at?" — is scoped to exactly one stream, because `append_batch`'s
expected-version argument names exactly one stream. That's the store's
actual atomicity boundary: one stream, one compare-and-append. There is no
multi-stream `append_batch` and no cross-stream expected-version check, so
there is nothing for a hypothetical "check stream B, then append to stream
A" to be atomic *against*. A command handler that manually `load`s a second
stream before calling `decide` can certainly read that stream's state — but
the read is stale the instant it returns; nothing pins stream B while stream
A's `append_batch` races to commit. That's a TOCTOU bug wearing a
transaction's clothes, not a transaction.

This is a direct consequence of the store's actual consistency boundary: one
stream is one linearizable history (`docs/spec/01-log-format.md`'s
per-stream position sequence), and that's the *only* boundary the backend
gives you for free. Anything that needs two aggregates' facts checked
together needs either a different stream boundary (§5) or an explicitly
asynchronous reconciliation step (§2, §3) — there is no third option that
stays honest about what "checked" means.

## 2. The default: accept, and reconcile downstream

The default posture is: **`decide` validates everything it can see (this
stream's own folded state), and accepts the rest.** "The rest" — typically a
foreign id referenced by the event, whose existence this stream cannot
verify — gets written as-is. Whatever downstream consumer cares about that
id's validity reconciles it later, on its own time, from the log.

Concretely, for `Follow { follower, target }` (see §4): `decide` checks
*is `follower` registered*, *is `follower != target`*, *is `target` not
already followed* — all lookups against `follower`'s own folded `User`
state. It does **not** check *is `target` a registered user* — that fact
lives on a stream this `decide` call never loaded and structurally cannot
load. The event commits with a `target` that, as far as the writer is
concerned, might not exist.

"Reconcile" doesn't have to mean *fix something*. The cheapest and most
common form is **reconcile at read time**: a projection that joins across
streams (`examples/social`'s `Projections`, which folds both `user-*` and
`post-*` stream families into one set of tables) simply treats a dangling
reference as "nothing to join" — a `PostView` for a post by an unregistered
author renders without a display name; a follow edge to a nonexistent user
never resolves into a rendered profile card anywhere, because nothing ever
looks it up successfully. No compensating write, no cleanup pass — the
projection's honest answer to "does the target exist" is just "not in my
tables", which is correct by construction, always, without an explicit
reconciliation step at all. This is the shape to reach for whenever the
downstream consequence of a dangling reference is *harmless staleness*, not
a violated invariant that some other party is relying on.

A stronger form is **reconcile by writing**: a projection or process that
notices an inconsistency and does something about it — mark the edge in a
side table as `unresolved`, exclude it from an aggregate count, queue it for
a background sweep. This is still "accept and reconcile", just with an
active downstream step instead of a passive read-time join. Reach for this
when the inconsistency needs to be *visible or actionable* to an operator or
another part of the system, but doesn't need to be *undone*.

## 3. The escalation: saga / compensating events

When reconciliation must actually **act on the source stream** — not just
adjust a read model, but retract the accepted fact because it turned out to
violate an invariant someone is relying on — the answer is a **process
manager that emits a compensating command through the same write path**.

`mess` has no saga/process-manager framework today; there's nothing to name
here beyond the primitive it would be built on: `Subscription`
(`crates/mess-store/src/subscription.rs`, a catch-up → live-tail cursor over
the global event stream, `docs/spec/06-subscriptions.md`). The pattern is: a
process subscribes to the global stream, reacts to the
event that made the unchecked reference (`Followed { target }`), checks the
referenced id against whatever *does* know about it (a projection, or a
direct load of that id's own stream), and — if the invariant is violated —
issues a **normal command** against the *originating* aggregate to undo it:

```rust
// sketch — not shipped code
while let batch = subscription.next_batch().await? {
    for rec in batch {
        if let Some(UserEvent::Followed { target }) = decode_if_user(&rec) {
            if !target_is_registered(target) {
                // compensating command, through the same command() path —
                // never a raw log append.
                store.command::<User, _>(&follower_stream, Unfollow { target }).await?;
            }
        }
    }
}
```

Two things make this a **saga**, not just "a cron job that fixes data":

- The compensation is itself a first-class domain command
  (`Unfollow`), going through `decide`/`append` like any other write — it is
  auditable in the log as "the system unfollowed this on your behalf",
  not a silent mutation.
- It's *eventually* consistent by design: there's a real window, bounded by
  how promptly the process manager reacts, during which the invariant is
  visibly violated (the follow edge exists and nothing has reconciled it
  yet). Anything that can't tolerate that window at all is not a
  cross-aggregate invariant candidate for this pattern — see §5.

This is strictly more machinery than §2, and correspondingly a strictly
worse default: it introduces a second write path with its own failure modes
(the process manager can itself crash mid-reconciliation, need its own
resumable cursor, etc.) purely to buy synchronous-*feeling* correction for
something the store fundamentally cannot check synchronously. Reach for it
only when §2's "let a read model be stale/absent" genuinely isn't good
enough — i.e., some other write path (not just a reader) depends on the
invariant actually holding.

## 4. Worked reference: `examples/social`'s `Follow`

`examples/social/src/domain/user.rs`'s module docs (the "Why the follow SET
lives in the *follower's* own aggregate" section) are the canonical
statement of §1 against real code: `Follow`'s `decide` checks
registration/self-follow/already-following against the follower's own
folded state, and its doc comment spells out, in the same terms as this
document, that checking the target's existence would need a "distributed
transaction — exactly what event sourcing trades away for per-stream
linearizability", and names the same two escalation paths as §2/§3.

`examples/social/src/projections.rs`'s fold of `Followed`/`Unfollowed`
events into `following`/`followers` maps is the running instance of §2's
read-time reconciliation: it records the edge by raw `Id` with no existence
check, so a dangling target simply never surfaces anywhere a real registered
user's profile is rendered. The demo doesn't need the write-time or saga
forms of reconciliation because nothing downstream currently depends on
"every followed id is real" being enforced — a legitimate example of §2
being sufficient on its own.

`examples/social/README.md`'s "Domain model" section makes the same point
at the crate level: "`User` and `Post` never reference each other's
aggregate state directly ... `decide` folds exactly one stream ... a
feature, not a gap."

## 5. When *not* to reach for this

Accept-and-reconcile (and its saga escalation) is for invariants that are
inherently facts about **two independently-owned aggregates**, where an
edge from one references the other. It is the wrong tool when the
invariant is really a fact about a **single relationship**, artificially
split across two aggregates that happen to be modeled at the wrong
granularity.

The tell: if a rule needs to check facts belonging to *both* parties in the
*same* `decide` call to be meaningful — not "eventually", not "as of the
last projection refresh", but atomically — then the fix is not a
reconciliation process bolted onto the read side. It's to **model the
relationship itself as its own stream**, so the one `decide` call that needs
both facts has a single stream to fold them from. A relationship keyed by
both participants (e.g. one stream per follow edge, per like, per
friendship) puts the check back inside §1's boundary instead of routing
around it.

This is exactly the direction goal **bn-gpq** takes `examples/social` in:
remodeling likes and follows as per-relationship streams instead of a set
living inside the `User`/`Post` aggregate. Once that lands, some of what
`Follow`/`Like` need to check moves from "impossible in `decide`, punt to
reconciliation" to "trivial in `decide`, because the relationship's own
stream is small and already holds both sides of the edge" — for whatever
subset of the invariant a relationship-stream can actually express alone
(a per-edge stream still can't, by itself, prove the *target user* exists
any more than today's model can, without also loading the target's own
stream — the existence check specifically is unaffected; what changes is
invariants about the *edge*, like "at most one active edge between this
pair"). This document's guidance is written to survive that change: it
names the *pattern* (model the shared fact as its own stream) rather than
pointing at specific lines of `user.rs`, because those lines are exactly
what bn-gpq's line of work is expected to move. When in doubt about which
posture a given cross-aggregate rule needs, ask: does correctness require
both facts to be checked in the same atomic step, or is "eventually
correct, harmlessly stale in between" acceptable? The former means model a
shared stream; the latter means §2, escalating to §3 only if the staleness
window is actionable, not just visible.
