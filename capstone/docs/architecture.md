# Architecture

Two views of the same system. The first is the system from outside. The second is what happens inside it: every exchange and queue, the retry path, the dead letter and invalid channels, the outcome events and the wire tap.

## System context

Orders arrive from three sources in three formats. The system calls one pricing service before it accepts an order, and four providers while it runs one. An operator reads the trace and the metrics, and acts on stuck sagas and dead-lettered messages.

```mermaid
flowchart LR
  web[Web shop<br/>JSON] ==>|POST /orders| system
  mobile[Mobile app<br/>JSON] ==>|POST /orders| system
  b2b[B2B partner<br/>XML, windows-1257] ==>|POST /orders| system

  system[["Order system<br/>intake · orchestrator · notifier · observer · RabbitMQ"]]

  system -->|price lookup| pricing[Pricing]
  system -->|authorize, refund| payment[Payment]
  system -->|reserve, release| inventory[Inventory]
  system -->|create| shipping[Shipping]
  system -->|send, notices| notification[Notification]

  operator([Operator]) -->|GET /trace, GET /ops/metrics| system
  operator -->|GET /dlq, POST /dlq/replay| system
  operator -->|GET /sagas/stuck, POST /sagas/:cid/compensate| system
```

## Integration architecture

Intake translates, prices and publishes `order.accepted` to `orders.exchange`. The orchestrator consumes `saga.queue` and runs the saga. It publishes lifecycle and outcome events to `orders.outcome`. Intake follows `#` there through its own `intake.state.queue`, which is not in the shipped topology, to keep `GET /orders` current. The notifier consumes `order.failed` and `order.compensated` from `notifications.queue`. Every service publishes hop records to `trace.events`, and the observer reads `trace.store`.

A temporary failure is rejected into the origin queue's retry queue. After one second it returns through `orders.return.exchange` to that queue only. After the third failed attempt the message goes to `orders.dlq.exchange` and then `orders.dlq`. A message that cannot be parsed goes to `orders.invalid.exchange` and then `orders.invalid`, and is never retried.

```mermaid
flowchart TB
  client([Client]) -->|POST /orders| intake
  intake -->|price lookup| pricing[Pricing]
  intake -->|order.accepted<br/>persistent, confirmed| ordersEx{{orders.exchange}}
  ordersEx --> sagaQ[(saga.queue)]

  sagaQ --> orch[orchestrator]
  orch -->|HTTP| mocks[payment · inventory · shipping · notification]
  orch -->|order.processing, order.completed, order.failed,<br/>order.compensated, order.parked, order.needs_attention| outcome{{orders.outcome}}

  outcome -->|#| stateQ[(intake.state.queue)]
  stateQ --> intake
  outcome -->|order.failed<br/>order.compensated| notifQ[(notifications.queue)]
  notifQ --> notifier
  notifier -->|notice| mocks

  sagaQ -.->|nack| retryEx{{orders.retry.exchange}}
  notifQ -.->|nack| retryEx
  retryEx -->|saga.queue| sagaRetry[(saga.retry.queue<br/>TTL 1 s)]
  retryEx -->|notifications.queue| notifRetry[(notifications.retry.queue<br/>TTL 1 s)]
  sagaRetry -->|saga.queue| returnEx{{orders.return.exchange}}
  notifRetry -->|notifications.queue| returnEx
  returnEx --> sagaQ
  returnEx --> notifQ

  orch -->|3rd failed attempt| dlqEx{{orders.dlq.exchange}}
  notifier -->|3rd failed attempt| dlqEx
  dlqEx --> dlq[(orders.dlq)]
  intake -->|POST /dlq/replay<br/>routing key = originQueue| returnEx
  dlq --> intake

  orch -->|unparseable| invEx{{orders.invalid.exchange}}
  notifier -->|unparseable| invEx
  observer -->|unparseable| invEx
  invEx --> invalid[(orders.invalid)]

  intake -->|hop| traceEx{{trace.events}}
  orch -->|hop| traceEx
  notifier -->|hop| traceEx
  traceEx --> traceQ[(trace.store)]
  traceQ --> observer
  observer -->|GET /trace/:cid<br/>GET /ops/metrics| operator([Operator])
```
