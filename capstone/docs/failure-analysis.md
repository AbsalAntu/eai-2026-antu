# Failure analysis

Ten failure scenarios, each reproduced on the running stack with the mocks' admin API. The trace excerpts are the operations of `GET /trace/<correlationId>` for that run. Order states come from `GET /orders/<correlationId>`, and effects from the mocks' `/admin/effects`.

## 1. Payment declined

**Trigger.** Payment answers `402 payment_declined` to every authorize call (`{"authorize":{"failMode":"always","failStatus":402}}` on port 4801). A web order is submitted.

**What the system does.** A 4xx is permanent, so nothing is retried. No step has completed, so nothing is compensated. The saga records `failed` with the mock's code and publishes `order.failed`. The notifier consumes that event and sends one `order_not_completed` notice. The first `saga.outcome` is `order.processing`, the second is `order.failed`.

```
intake.receive      success
intake.translate    success
intake.enrich       success
intake.publish      success
saga.consume        success
saga.outcome        success   (order.processing)
payment             failed
saga.outcome        success   (order.failed)
notify.consume      success
notify.send         success
```

**Final state.** `failed`, code `payment_declined`.

**Alert.** None, by design. The order reached a terminal state and the customer was told. Nothing is dead-lettered, stuck or queued up.

**Operator action.** None. A second attempt with the same `orderId` is a duplicate, because the saga dedupes on the business key in every state, so the customer needs a new order id. ADR-008 discusses that cost.

## 2. Inventory unavailable

**Trigger.** Inventory answers `422 inventory_unavailable` to every reserve call (`{"reserve":{"failMode":"always"}}` on port 4802). A web order is submitted.

**What the system does.** Payment succeeds and inventory fails. The saga compensates the one completed step with a refund, records `compensated` with the failing call's code, and publishes `order.compensated`. The notifier sends the notice. Public test P3.2 checks that payment holds exactly one refund.

```
saga.consume        success
saga.outcome        success   (order.processing)
payment             success
inventory           failed
payment_refund      success
saga.outcome        success   (order.compensated)
notify.consume      success
notify.send         success
```

**Final state.** `compensated`, code `inventory_unavailable`.

**Alert.** None, by design. The saga finished cleanly in a terminal state.

**Operator action.** None for this order. Check the stock of the product, because every order for it fails the same way.

## 3. Shipping timeout

**Trigger.** Shipping waits 3500 ms before it answers (`{"create":{"delayMs":3500}}` on port 4803). That is longer than `REQUEST_TIMEOUT_MS`, which is 2500 ms. A web order is submitted.

**What the system does.** Payment and inventory succeed. The shipping call gives up after 2500 ms and is recorded as `timeout`. The saga compensates in reverse order: `inventory_release` first, then `payment_refund`. It records `compensated` with the code `timeout` and publishes `order.compensated`. The notifier sends the notice.

```
saga.consume        success
saga.outcome        success   (order.processing)
payment             success
inventory           success
shipping            timeout
inventory_release   success
payment_refund      success
saga.outcome        success   (order.compensated)
notify.consume      success
notify.send         success
```

**Final state.** `compensated`, code `timeout`.

**Known defect, observed.** The order is `compensated`, but shipping's effects list **one shipment** for it. A delayed call performs its effect when the delay ends, even if the caller has stopped waiting. Shipping has no compensating endpoint, so the system cannot undo that shipment. The saga's state says nothing was shipped, and the carrier disagrees.

**Alert.** None. The saga finished in a terminal state, so no alert condition is met.

**Operator action.** Ask the carrier whether a shipment exists for the order id, and cancel it by hand if it does.

## 4. A refund fails, then operator compensation

**Trigger.** Shipping fails every call (`{"create":{"failMode":"always"}}` on port 4803) and so does the refund (`{"refund":{"failMode":"always"}}` on port 4801). A web order is submitted.

**What the system does.** Payment and inventory succeed and shipping fails with `shipping_unavailable`. Compensation runs in reverse order. `inventory_release` succeeds, `payment_refund` fails, and the saga stops. It records `compensation_failed` with the code of the call that failed, `refund_failed`, and publishes `order.compensation_failed`. The shipped bindings send only `order.failed` and `order.compensated` to the notifier, so the customer gets **no notice** at this point. `GET /sagas/stuck` lists the order with `completedSteps` payment and inventory, and `compensatedSteps` inventory_release.

The operator fixes the refund (`{"refund":{"failMode":"never"}}`) and calls `POST /sagas/<cid>/compensate`. It runs **only the compensation that has not succeeded**: the refund. The release is not repeated. The order becomes `compensated` with `shipping_unavailable`, the code of the failure that started the compensation. Now the notifier sends its notice.

```
shipping                   failed
inventory_release          success
payment_refund             failed
saga.outcome               success   (order.compensation_failed)
-- operator: POST /sagas/<cid>/compensate --
payment_refund             success
saga.operator_compensate   success
saga.outcome               success   (order.compensated)
notify.consume             success
notify.send                success
```

The mocks' effects after the repair: one charge, one refund, one reservation and one release.

**Final state.** First `compensation_failed` (code `refund_failed`), then `compensated` (code `shipping_unavailable`).

**Alert.** `stuck_sagas` fires while the order is `compensation_failed`, because the observer counts every saga that `GET /sagas/stuck` lists (R-4.9.2). I read the rule from the code and did not read `/ops/metrics` during this run. Scenario 7 shows the same alert firing.

**Operator action.** Fix the cause of the refund failure with the payment provider. Call `POST /sagas/<cid>/compensate`. A second call returns the same body and runs nothing. Known gap: the customer is not told while the order is `compensation_failed`.

## 5. A payment outage before step 1: retry, parked, replay

**Trigger.** Payment answers `503` to every authorize call (`{"authorize":{"failMode":"always","failStatus":503}}` on port 4801). A web order is submitted.

**What the system does.** Payment is the first step, so a 5xx means nothing is known: the provider might have charged. The orchestrator writes a `saga.retry` hop and rejects the message into the retry path. It waits one second and returns to `saga.queue` only. The third attempt writes `saga.dead_letter`, publishes the message to `orders.dlq.exchange` with `originQueue: saga.queue` and `lastError: payment 503`, and the state becomes `parked`. `GET /dlq` shows the entry with `attempts: 3`.

The operator fixes payment and calls `POST /dlq/replay` with the order's `correlationId`. The answer is `{"replayed":1,"remaining":0}`. The message returns to `saga.queue` with a fresh retry history, and the saga runs. Every call carries the key `authorize:web:<orderId>`, so payment shows exactly **one charge**.

```
saga.consume        success
saga.outcome        success   (order.processing)
payment             failed
saga.retry          retried
saga.consume        success   (attempt 2)
payment             failed
saga.retry          retried
saga.consume        success   (attempt 3)
payment             failed
saga.dead_letter    dead_lettered
saga.outcome        success   (order.parked)
dlq.replay          success
saga.consume        success
saga.outcome        success   (order.processing)
payment             success
inventory           success
shipping            success
notification        success
saga.outcome        success   (order.completed)
```

**Final state.** `parked`, then `completed` after the replay.

**Alert.** `dlq_not_empty` fires while the message is in `orders.dlq` and clears after the replay.

**Operator action.** Restore payment, then `POST /dlq/replay` with `{"correlationId":"<cid>"}`. If the cause is not fixed, the message gets its three attempts again and returns to the DLQ.

## 6. A failure-notice delivery outage

**Trigger.** Payment declines every order (`{"authorize":{"failMode":"always","failStatus":402}}` on port 4801), so each order produces an `order.failed` event. The notification mock answers `503` to every `order_not_completed` notice (`{"send":{"failMode":"always","failStatus":503,"failForKinds":["order_not_completed"]}}` on port 4804). A web order is submitted.

**What the system does.** The saga ends `failed` with `payment_declined`. The notifier consumes `order.failed` and the notice call fails. It writes a `notify.retry` hop and rejects the message into the retry path, and after one second it returns to `notifications.queue` only. The third failed attempt writes `notify.dead_letter` and publishes the message to `orders.dlq.exchange` with `originQueue: notifications.queue` and `lastError: notification 503`. The saga's state does not change: the notice is a side effect of the outcome, not a step of the saga.

```
saga.outcome        success   (order.failed)
notify.consume      success
notify.send         failed
notify.retry        retried
notify.consume      success   (attempt 2)
notify.send         failed
notify.retry        retried
notify.consume      success   (attempt 3)
notify.send         failed
notify.dead_letter  dead_lettered
```

`GET /dlq` lists the entry with `attempts: 3`. The operator fixes the notification service and calls `POST /dlq/replay` with the order's `correlationId`. The answer is `{"replayed":1,"remaining":0}`. The message returns to `notifications.queue` with a fresh retry history, and one notice is sent.

```
dlq.replay          success
notify.consume      success
notify.send         success
```

The notification mock holds exactly **one** `order_not_completed` notice for the order.

**Final state.** The order stays `failed` (`payment_declined`). Only the notice moves: undelivered, then dead-lettered, then delivered after the replay.

**Alert.** `dlq_not_empty` fires while the entry is in `orders.dlq`: `/ops/metrics` showed value 1 and `firing: true`. It clears after the replay.

**Operator action.** Restore the notification service, then `POST /dlq/replay` with `{"correlationId":"<cid>"}`. If the cause is not fixed, the message gets three attempts again and returns to the DLQ. Notices are at-least-once by design: when the provider sent a notice but the call reported a failure, a retry or a replay may send a second one, and the provider offers no idempotency key to prevent it.

## 8. A broker restart

**Trigger.** `docker stop cap-rabbitmq`, a wait, then `docker start cap-rabbitmq`.

**What the system does.** While the broker is down, each of the four services answers `503` on `GET /health`, because it has no connection and consumes nothing. `POST /orders` answers `503 broker_unavailable` with `Retry-After: 1`, and the intake keeps no idempotency record for the failed request, so the same key works once the broker is back. Each service reconnects in its own loop, declares again what it declares, and consumes again. The processes do not exit: `RestartCount` was 0 for all four before and after, and the container start time did not change. Forty seconds after `docker start`, all four `/health` answered `200`, and a new order ran to `completed`.

```
before:   intake=0  orchestrator=0  notifier=0  observer=0   (RestartCount)
down:     8800 health 503   8801 health 503   8802 health 503   8803 health 503
          POST /orders -> 503 {"code":"broker_unavailable"}  Retry-After: 1
after:    8800 health 200   8801 health 200   8802 health 200   8803 health 200
          RestartCount: all 0
new order: intake.receive ... intake.publish, saga.consume, payment, inventory,
           shipping, notification, saga.outcome   -> completed
```

**Final state.** Orders already `completed` or terminal are untouched, because the stores are files in named volumes. A new order completes after the reconnect.

**Alert.** None can fire, by design. The seven alerts are computed by the observer from the broker's queue figures, and the observer has no broker connection during the outage. From the code, `GET /ops/metrics` answers `503` then. I did not read it during this run. The signal that exists is `503` on every `/health`.

**Operator action.** Start the broker. Wait up to 30 seconds. Check that every `/health` answers `200`. Then check `GET /ops/metrics` for `saga_consumer_missing` and `notifier_consumer_missing`: both must be quiet.

## 9. A duplicate submission with a new Idempotency-Key

**Trigger.** The same order (same `orderId`, same body) is submitted twice, with two different Idempotency-Keys. The client key cannot catch this, because it is a different key.

**What the system does.** Intake accepts both: each gets its own `correlationId`. The saga is the layer that catches it. It dedupes on the business key `web:<orderId>`. The first order runs. The second finds the business key owned by another `correlationId`, records the state `duplicate` with `duplicateOf`, writes a `saga.dedupe` hop with status `duplicate`, publishes `order.duplicate`, and runs **no step**. Payment shows exactly one charge. No notice is sent for the duplicate, because only `order.failed` and `order.compensated` reach the notifier.

```
first order:   intake.receive ... intake.publish, saga.consume, saga.outcome,
               payment, inventory, shipping, notification, saga.outcome  -> completed

second order:  intake.receive
               intake.translate
               intake.enrich
               intake.publish
               saga.consume
               saga.dedupe        duplicate
               saga.outcome       success   (order.duplicate)
```

**Final state.** First order `completed`. Second order `duplicate`, with `duplicateOf` set to the first order's `correlationId`.

**Alert.** None, by design. A duplicate is a normal, expected outcome and is terminal.

**Operator action.** None. Cost to know: the dedupe holds in every state of the first order. A customer whose order was declined cannot resubmit it with the same `orderId`: the second attempt is a `duplicate` of a `failed` order. The customer needs a new order id. ADR-008 discusses this.

## 10. A malformed message

**Trigger.** The bytes `{ not json` are published to `orders.exchange` with a `correlationId` header and `delivery_mode` 2, through the management API.

**What the system does.** Only `saga.queue` is bound to `orders.exchange`, so the orchestrator is the one consumer that sees it. It parses the body inside its `try` block. The parse fails, so the message is permanent: the orchestrator publishes it to `orders.invalid.exchange` with its **original bytes and headers**, writes an `invalid.quarantine` hop, and acks it. It is never retried and never reaches `orders.dlq`. Because the message has a `correlationId` header, the hop is written. A malformed message without that header is quarantined without a hop. The notifier and the observer follow the same rule on their own queues.

```
GET /trace/11111111-1111-4111-8111-111111111111
  orchestrator  invalid.quarantine  quarantined
```

No `saga.retry` hop appears for it. `orders.invalid` held 1 message, and `/ops/metrics` showed `invalid_not_empty` with value 1 and `firing: true`.

**Final state.** The message sits in `orders.invalid`. No order exists for it.

**Alert.** `invalid_not_empty` fires while any message is in `orders.invalid`.

**Operator action.** Find the producer that sent the bytes and fix it. Nothing replays `orders.invalid`: a message nobody can parse fails the same way every time, so a second delivery would not help. After the producer is fixed, purge the queue so the alert clears.

