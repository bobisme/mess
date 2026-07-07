# 06 — Algebraic event model

## Why algebra matters here

The storage engine should be fast. The developer experience should be safe. Algebra is the bridge:

```text
if we can model events, snapshots, cursors, and projections with simple laws,
we can generate APIs, tests, optimizers, and distributed semantics without hand-wavy bullshit.
```

## Events form a free monoid

For a stream with event type `E`, event histories are sequences:

```text
E* = all finite lists of E
```

Operation:

```text
concat: E* × E* -> E*
identity: []
```

Laws:

```text
xs ++ [] = xs
[] ++ xs = xs
(xs ++ ys) ++ zs = xs ++ (ys ++ zs)
```

This gives the basic replay law.

## Aggregates are monoid actions

An aggregate state `S` and event sequence monoid `E*` form a right action:

```text
act: S × E* -> S
```

with laws:

```text
act(s, []) = s
act(s, xs ++ ys) = act(act(s, xs), ys)
```

This is exactly event sourcing.

Snapshot correctness is just action associativity:

```text
full replay:
  act(s0, prefix ++ suffix)

snapshot + tail:
  act(act(s0, prefix), suffix)

same by law.
```

## Command handling as a partial function

A command handler is:

```text
decide: S × Command -> Result<E*>
```

A commit is valid when:

```text
current_stream_version == expected_version
```

Then emitted events append to the stream.

This is the safe DX shape:

```rust
trait Aggregate: Default {
    type Event;
    fn apply(&mut self, event: &Self::Event);
}

trait Decide<C>: Aggregate {
    fn decide(&self, command: C) -> Result<Vec<Self::Event>>;
}
```

Generated store flow:

```text
load S
events = decide(S, command)
append(events, expected_version = loaded_version)
```

## Projections are folds

A projection over event type `E` and state `P`:

```text
project: P × E -> P
```

Global/category projection:

```text
P_n = fold(project, P_0, events[0..n])
```

Checkpoint stores `n` or a cursor frontier.

## Projection composition

If two projections read the same event stream:

```text
p1: P1 × E -> P1
p2: P2 × E -> P2
```

Their product projection is:

```text
p12: (P1, P2) × E -> (P1, P2)
p12((a,b), e) = (p1(a,e), p2(b,e))
```

This is useful for batching and replay: one pass over the log can update many projections.

## Homomorphisms and summarization

A summary function `h: E* -> M` is a monoid homomorphism if:

```text
h([]) = identity_M
h(xs ++ ys) = h(xs) <> h(ys)
```

Examples:

```text
count events
sum deposited amounts
max timestamp
set of touched stream IDs
Bloom/Ribbon filter construction for sealed segment, if treated as build-once summary
```

Segment summaries are homomorphisms. That means they can be built incrementally and merged hierarchically.

## Semirings for provenance and incremental computation

Database provenance research uses semirings to annotate tuples and propagate annotations through relational operators. This matters because projections can be interpreted as “event deltas -> materialized view deltas.”

Practical translation:

```text
addition: alternative ways a result can be produced
multiplication: joint dependency/composition
```

For Mess, semiring-style annotations could track:

```text
which events contributed to a projection row
whether a row is reconstructible after retention
confidence/cost/lineage metadata
```

Do not implement this in v1. But the model is useful for future projection debugging: “why does this read model row exist?”

## Strict vs relaxed streams

The old prototype already hints at two stream-position modes: sequential and relaxed. Make that real.

### Strict stream

```text
events are totally ordered by stream_version
append requires expected_version
fold order matters
```

Use for aggregates with invariants:

```text
bank account
order lifecycle
state machine
inventory allocation unless escrowed
```

### Relaxed stream

```text
events form a partially ordered or unordered set
merge/apply must be commutative enough
```

Use only when the event algebra proves it:

```text
counters
sets
analytics increments
idempotent facts
CRDT-like structures
```

## CRDT and semilattice basis

CRDTs let replicas accept updates without coordination and converge when they have the same updates, using mathematically defined merge rules. State-based CRDT convergence is guaranteed when states form a join-semilattice, updates inflate state, and merge computes least upper bound [CRDT overview](https://arxiv.org/abs/1805.06358).

Operation-based CRDTs converge when operations are reliably delivered and either delivered in causal order with concurrent operations commuting, or all operations commute if causal order is not preserved [CRDT overview](https://arxiv.org/abs/1805.06358).

For Mess relaxed streams:

```rust
trait RelaxedEvent {
    type State;

    /// Must be associative, commutative, and idempotent if delivered unordered.
    fn merge(state: &mut Self::State, event: Self);
}
```

Better:

```rust
trait JoinSemilattice {
    fn join(&mut self, other: Self);
}
```

## CALM theorem as product guide

CALM says programs with consistent, coordination-free distributed implementations are exactly the programs expressible in monotonic logic [Keeping CALM](https://arxiv.org/abs/1901.01930).

Product implication:

```text
If a user’s projection/aggregate is monotonic, Mess can eventually distribute it with less coordination.
If it is non-monotonic, Mess should force a coordination boundary or strict stream.
```

Possible DX:

```rust
#[projection(monotonic)]
struct LikesByPost;

#[aggregate(strict)]
struct BankAccount;

#[aggregate(relaxed, crdt = "PNCounter")]
struct ViewCounter;
```

## Invariant confluence

Bailis et al.’s invariant confluence framework determines when an application requires coordination for correctness and gives a necessary and sufficient condition for safe coordination-free execution over application-level invariants [Coordination Avoidance](https://arxiv.org/abs/1402.2237).

This is the path to future globally distributed Mess without lying:

```text
not every aggregate can be relaxed
not every invariant needs consensus
some invariants can be preserved with escrow/rights allocation
```

Example:

```text
counter >= 0
```

Naive decrement requires coordination. Escrow can split decrement rights among replicas, letting most decrements proceed locally until rights are exhausted. CRDT literature calls this out for bounded counters [CRDT overview](https://arxiv.org/abs/1805.06358).

## Causal frontiers

For multi-shard/distributed subscriptions, a cursor should become a frontier:

```text
F: ShardId -> Position
```

Merge:

```text
join(F, G)[s] = max(F[s], G[s])
```

This is a join-semilattice and therefore safe to merge from multiple workers.

## Algebraic API ideas

### Event laws as tests

Generated property tests:

```text
snapshot law:
  fold(s0, xs ++ ys) == fold(fold(s0, xs), ys)

idempotent projection law, if declared:
  apply(apply(s,e),e) == apply(s,e)

commutative relaxed law, if declared:
  apply(apply(s,a),b) == apply(apply(s,b),a)
```

### Compile-time markers

```rust
trait StrictAggregate {}
trait MonotonicProjection {}
trait CommutativeEvent {}
trait IdempotentEvent {}
```

Do not trust markers blindly. Use property tests and optional model checking.

### Event schemas as algebraic data types

Rust enums are ideal:

```rust
#[derive(Event)]
enum PostEvent {
    Posted { body: String },
    HiddenByPoster,
    HiddenByModerator,
}
```

The derive can generate:

```text
event type IDs
codec
schema fingerprint
compatibility checks
test fixtures
From<Message>
```

## What is novel enough to pursue

### Fold certificates

Snapshots carry cryptographic proof of the exact event prefix they summarize.

### Coordination annotations

Aggregate/projection traits declare strict, monotonic, commutative, idempotent, or escrowed semantics. The runtime chooses coordination/storage strategy accordingly.

### Cursor frontiers as semilattices

Consumer progress uses join-semilattice frontiers from day one, even on single node.

This makes the distributed path less painful later.

