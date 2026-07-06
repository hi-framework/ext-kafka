# ext-kafka Code Review Findings

Date: 2026-07-02

This document records the static review findings for `ext-kafka/`. The local
environment did not provide `cargo` or `git`, so these findings were not
validated by running the Rust test suite or checking the worktree diff.

## Current status

- Fixed: `produceFnf` now returns after local librdkafka enqueue instead of
  waiting for broker delivery.
- Fixed: public PHP integer parameters are range-checked before conversion.
- Fixed: transactional producers now track idle/transitioning/active state and
  serialize transactional produce / sendOffsets / commit / abort operations.
- Fixed: `produceSync` timeout is now carried in the `ProduceReq` IPC payload.
  The worker returns a structured `TIMEOUT` error if the delivery report does
  not arrive within that window. Note: librdkafka may still complete delivery
  after the future is dropped, so callers should still use idempotent producer
  settings when retrying after timeout.

## 1. `produceFnf` waits for broker delivery

**Location**

- `worker/src/producer/real.rs`
- `ext/src/ipc.rs`

**Problem**

`KafkaProducer::produce_fnf()` calls `producer.send(...).await`, which waits for
the broker delivery report. The API and comments describe fire-and-forget as
"enqueue success means success", but the implementation blocks until delivery.
The extension side also waits for a `ProduceResp` with a fixed 5 second timeout.

**Impact**

When the broker is slow or unavailable, fire-and-forget calls can block and then
time out on the PHP side. If the caller retries after the IPC timeout, the worker
may still complete the original delivery later, creating duplicate produce risk.

**Suggested fix**

Use a producer path that only performs synchronous local enqueue validation for
FNF, such as an rdkafka enqueue/send-result style API, and return after the
worker has accepted the message locally. Keep broker delivery waiting only for
`produceSync`.

## 2. Transaction locking does not cover the transaction lifecycle

**Location**

- `worker/src/producer/real.rs`

**Problem**

`txn_lock` is held only inside individual operations. In particular,
`begin_transaction()` acquires the lock and releases it before returning, while
`commit_transaction()` and `abort_transaction()` do not hold it. The code
comments say same-cluster transactions are serialized, but concurrent PHP
requests can still interleave `begin`, `produce`, `sendOffsetsToTransaction`,
and `commit` on the same producer.

**Impact**

Concurrent transaction flows can corrupt operation ordering, trigger librdkafka
transaction state errors, or place messages and offsets into the wrong
transaction window.

**Suggested fix**

Introduce an explicit transaction state machine or transaction lease/owner that
is acquired at `beginTransaction` and released only at `commitTransaction` or
`abortTransaction`. Reject or queue conflicting operations for the same cluster.

## 3. Public PHP integer parameters can silently wrap or truncate

**Location**

- `ext/src/lib.rs`
- `ext/src/client.rs`

**Problem**

Several PHP-facing `i64` values are converted with `as`, for example:

- `subscriptionId as u64`
- `timeoutMs as u32`
- `maxMessages as u32`
- `partition as i32`

Negative subscription IDs become huge unsigned values. Large timeout or message
count values can truncate. Out-of-range partitions can wrap into a different
partition value.

**Impact**

Invalid user input can become a valid but unintended request instead of failing
fast with a clear PHP exception. This makes debugging difficult and can target
the wrong subscription or partition.

**Suggested fix**

Validate public inputs before conversion. Reject negative IDs, reject or clamp
values above protocol limits, and return `InvalidArgument`/`PhpException` for
invalid ranges.

## 4. `produceSync` timeout did not reach worker-side delivery

**Location**

- `ext/src/ipc.rs`
- `worker/src/server.rs`
- `worker/src/producer/real.rs`

**Problem**

Before the fix, PHP `timeoutMs` controlled only IPC read/write timeout. The
worker still awaited the Kafka delivery future, controlled by librdkafka settings
such as `message.timeout.ms`.

**Impact**

The PHP caller could receive a timeout while the worker continued waiting on the
same delivery future. Retrying at the application layer could duplicate
messages, especially when idempotence was not configured.

**Suggested fix**

The timeout is now passed through `ProduceReq` and enforced around the worker
delivery wait. This returns a structured `TIMEOUT` error before the PHP-side IPC
timeout fires. Because librdkafka does not provide per-message cancellation
after enqueue, timeout still does not prove the message was not delivered.

## 5. Verification gap in the current environment

**Problem**

The local environment lacks `cargo`, so `cargo test --workspace --no-run` could
not be executed. It also lacks `git`, so the current worktree state could not be
checked.

**Suggested tests**

- Fire-and-forget should return after local enqueue and not wait for broker
  delivery.
- Concurrent transactions on the same cluster should be rejected or serialized
  across the full begin-to-commit/abort lifecycle.
- Negative and out-of-range PHP parameters should raise clear argument errors.
- `produceSync` timeout semantics should be covered, including whether worker
  delivery continues after client timeout.
