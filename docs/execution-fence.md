# Generation-fenced execution and lease recovery

`animus.execution-fence.v1` is the ownership contract for resilient Animus
execution. A workflow id is a locator; it is not sufficient authority after a
daemon crash, lease expiry, or duplicated delivery.

## Identity

An `ExecutionFence` binds:

- one stable workflow id and positive workflow generation;
- zero or one canonical qualified subject id and positive subject generation;
- for queue-backed work, one queue entry, daemon owner, positive lease
  generation, and backend-clock expiry;
- for coding work, one canonical repository plus fully-qualified base and head
  refs.

Every boundary carries the same envelope: queue lease, journal bootstrap,
workflow-runner request/result, remote `exec_session`, retained environment
record, publication receipt validation, and fleet status projection. Coding
execution fails closed before workspace preparation when the subject or
repository reservation is absent.

## Queue rules

The generation-aware surface is `queue/v2/*`; legacy `queue/*` methods remain
separate for compatibility.

1. `queue/v2/enqueue` allocates a monotonic subject generation. A repeated
   producer idempotency key returns the original entry and generation.
2. `queue/v2/lease` selects only Pending entries. It attaches a stable workflow
   id once and returns a complete `ExecutionFence`.
3. A live holder renews by compare-and-swap on entry id, workflow and subject
   generations, owner id, and lease generation.
4. Expiry never returns an Assigned entry to ordinary leasing. It produces
   `expired_lease_recovery_required` until the scheduler probes the exact
   journal, runner, environment handle, and node.
5. If recovery is safe, `queue/v2/lease/recover` transfers ownership to the new
   daemon and increments only the queue lease generation. Workflow and subject
   generations remain unchanged.
6. Completion and return-to-pending require the current full fence. A stale
   daemon receives `stale_fence` and cannot terminalize or reschedule the work.
7. Return-to-pending preserves the canonical workflow id/generation so a spawn
   defer or recoverable infrastructure failure cannot silently mint a new run.

## Collision rules

Schedulers reject the exact same subject generation and any active reservation
with the same normalized repository/head-ref collision key. Independent
repositories or independent head refs may consume separate fleet slots up to
the configured capacity.

## Publication

`PublicationReceipt::validate_against_execution` verifies workflow id and
generation, qualified subject and generation, and the reserved head ref in
addition to the receipt's commit/tree/remote/PR proof. A valid receipt from a
prior generation cannot authorize cleanup of the current workspace.

## Migration

Generation-aware coding must negotiate the queue, runner, and environment
capabilities and fail closed when any required fence is absent. Deploy the
protocol and plugin implementations before enabling the daemon/Portal scheduler.
Do not infer generations from timestamps, workflow names, phase names, or a
live node name. Legacy runs may remain readable, but they cannot satisfy the
generation-fenced coding policy.
