# Runbook

What an operator does when an alert fires. The alerts come from `GET /ops/metrics` on the observer (port 8801). Each section gives the meaning, the likely causes, how to diagnose, the fix, and how to verify.

## dlq_not_empty

**Meaning.** A message failed `MAX_RETRIES` (3) attempts and was dead-lettered. `orders.dlq` holds it, and `dlqDepth` is above 0.

**Likely causes.**
- A provider was down. The orchestrator dead-letters a first-step failure (payment, or inventory for b2b) from `saga.queue`. The notifier dead-letters a failed notice from `notifications.queue`.
- The cause is permanent but looked temporary, for example a provider that answers `503` for one product only.

**Diagnose.**
1. `curl http://localhost:8800/dlq` lists each entry with `originQueue`, `attempts`, `lastError` and `correlationId`. It is non-destructive.
2. `curl http://localhost:8801/trace/<correlationId>` shows the retries (`saga.retry` or `notify.retry`) and the final `saga.dead_letter` or `notify.dead_letter` hop.
3. `curl "http://localhost:4801/admin/logs?orderId=<orderId>"` on the provider's port shows what it received and answered.
4. `docker compose logs --tail 40 orchestrator` or `notifier`.

**Fix.**
- Provider outage: restore the provider, then `curl -X POST http://localhost:8800/dlq/replay -H "Content-Type: application/json" -d '{"correlationId":"<cid>"}'`. With `{}` it replays every entry.
- Cause not fixed: do not replay. The message gets three more attempts and returns to the DLQ. Fix the cause first.
- A replay is safe for an order whose first step failed, because every retry repeats the same provider key (`authorize:<businessKey>`), so a repeat never charges twice. A replayed notice may be sent twice if the first one was delivered but reported as failed. Notices are at-least-once by design.

**Verify.** The replay answers `{"replayed":N,"remaining":0}`. `GET /dlq` shows `count` 0. `dlqDepth` returns to 0, and `dlq_not_empty` stops firing. `GET /orders/<cid>` shows `completed`, and the provider's `/admin/effects` shows one effect for the order.

## invalid_not_empty

**Meaning.** A message could not be processed at all, and `orders.invalid` holds it. `invalidDepth` is above 0.

**Likely causes.**
- A producer sent a body that is not valid JSON, or a body that is not a canonical order.
- A message arrived without a `correlationId` header. That is permanent too, because no consumer can trace it.

**Diagnose.**
1. Open the management UI at http://localhost:15680 (user `eai`) and read the message in `orders.invalid`. Its original bytes and headers are untouched.
2. If it has a `correlationId` header, `curl http://localhost:8801/trace/<correlationId>` shows an `invalid.quarantine` hop and which service quarantined it. A message with no header has no hop.
3. `docker compose logs --tail 40 orchestrator`, `notifier` and `observer`.

**Fix.** Nothing replays `orders.invalid`, and that is deliberate (R-4.6.7). A message nobody can parse fails the same way every time, so a second delivery only repeats the failure. Find the producer that sent it and fix the producer. Then purge the queue so the alert clears: `curl -u eai:eai-capstone -X DELETE http://localhost:15680/api/queues/%2F/orders.invalid/contents`. If the order matters, ask the client to submit it again through `POST /orders`.

**Verify.** `invalidDepth` returns to 0 and `invalid_not_empty` stops firing. The producer's next message reaches `saga.queue` and runs.

## saga_consumer_missing

**Meaning.** `saga.queue` has no consumer. Accepted orders pile up in the queue, and none of them runs.

**Likely causes.**
- The orchestrator container is stopped or crashed (`docker compose ps` shows `Exited`).
- The orchestrator lost its broker connection and has not reconnected yet. After a broker restart it takes up to 30 seconds.

**Diagnose.**
1. `docker compose ps` shows the state of `cap-orchestrator`.
2. `curl http://localhost:8802/health` answers `503` with `consumers: {"saga.queue": 0}` when it is not consuming.
3. `docker compose logs --tail 40 orchestrator` shows `broker not available` while it retries, or the error that stopped it.
4. `curl http://localhost:8801/ops/metrics` shows `saga.queue` with `ready` growing and `consumers` 0.

**Fix.** If the container is stopped, `docker compose up -d orchestrator`. If it is running but not consuming, wait for the broker, then check the logs. If a code error stops it at start, fix it and rebuild with `docker compose up -d --build --wait`. Orders waiting in `saga.queue` run when the consumer returns: nothing is lost, because the messages are persistent.

**Verify.** `GET /health` on port 8802 answers `200` with `saga.queue` at 1. `saga_consumer_missing` stops firing, `ready` drops to 0, and the waiting orders reach `completed`.

cat >> docs/runbook.md <<'EOF'
## stuck_sagas

**Meaning.** One or more sagas are in `needs_attention` or `compensation_failed`. `GET /sagas/stuck` lists them. Nobody finishes them automatically.

**Likely causes.**
- `needs_attention`: the orchestrator was killed or restarted while a saga was running. After the restart, a saga whose steps ran under the previous run is marked stuck, and none of its steps is run again. A clean `docker restart` mid-saga looks the same, so it is a false positive for an order that was about to finish.
- `compensation_failed`: a compensation call (the refund or the release) failed, and the remaining ones were not attempted.

**Diagnose.**
1. `curl http://localhost:8802/sagas/stuck` lists each saga with `completedSteps` and `compensatedSteps`.
2. `curl http://localhost:8801/trace/<correlationId>` shows the last hop that ran.
3. The providers' `/admin/effects` for the order id show what really happened. A call that timed out on our side may still have had its effect. For example, shipping finishes a delayed call even after the saga has given up on it, and shipping has no undo.

**What to know before you press compensate.**
- It runs only the compensations that have **not** yet succeeded, in reverse order, with the provider keys. It never repeats one that succeeded. A repeat of the call returns the same answer and runs nothing.
- It cannot undo shipping or the notification. If a shipment exists, ask the carrier to cancel it. Check the shipping provider's effects first.
- When the saga was stuck with no failed step, the result is `compensated` with the code `operator_compensated`. When it follows a failed compensation, the result keeps the code of the failure that started it.
- For `compensation_failed`, fix the cause first (for example the refund provider), or the call fails again and the order returns to `compensation_failed`.

**Fix.** `curl -X POST http://localhost:8802/sagas/<correlationId>/compensate`. The answer is `{"correlationId":"...","state":"compensated"}`. If it answers `409 compensation_in_progress`, another call is running. If it answers `409 saga_not_stuck`, the saga is not stuck.

**Verify.** `GET /sagas/stuck` no longer lists the order. `GET /orders/<correlationId>` shows `compensated`. The payment provider shows one charge and one refund, and the inventory provider one reservation and one release. `stuck_sagas` stops firing.
