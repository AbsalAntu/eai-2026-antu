# Patterns

The integration patterns in this system, and where each one happens in the code. Each row names the line where it happens.

| Pattern | Problem | Where | Why |
|---|---|---|---|
| Normalizer | Three sources send three formats (web JSON, mobile JSON, b2b XML in windows-1257), and the rest of the system wants one. | [server.ts:94](../intake/server.ts#L94) | One endpoint detects the format from the content type and body, so no consumer ever sees a source format. |
| Message Translator | The mobile order carries an epoch, a numeric currency and a one-string address, and the b2b order carries XML attributes. | [translate.ts:105](../intake/translate.ts#L105) | Each source has its own translator function, so a change to one format touches one function. The b2b one is at [translate.ts:149](../intake/translate.ts#L149). |
| Content Enricher | The orders carry no prices, so the amount cannot be computed from them. | [server.ts:67](../intake/server.ts#L67) | Intake asks the pricing service for every product, so a price always comes from the catalog. The decorative `UnitListPrice` in the b2b file is never read. |
| Content Filter | Every source carries payment data, and the canonical order has no place for it. | [server.ts:166](../intake/server.ts#L166) | The order is built from a list of fields and then validated against the canonical schema, so payment data cannot leak by default. |
| Canonical Data Model | Three formats would mean three versions of every consumer. | [server.ts:166](../intake/server.ts#L166) | Every order is validated against `canonical/order.schema.json` before it is published, and the event body is never extended. Identifiers travel in headers. |
| Idempotent Receiver | A client retries a request, or sends it twice by mistake, and the order would be accepted twice. | [server.ts:235](../intake/server.ts#L235) | The Idempotency-Key and the SHA-256 of the raw body decide: the same key and body replay the stored answer, and a changed body is a 409. |
| Guaranteed Delivery | An order acknowledged to the client must not be lost if the broker or a service fails. | [server.ts:183](../intake/server.ts#L183) | The event is persistent and published with a broker confirm. Intake answers 202 only after the confirm. |
| Correlation Identifier | One order crosses four services, and its log lines are scattered. | [trace.ts:23](../shared/trace.ts#L23) | Every hop carries the `correlationId` and its cause, so `GET /trace/<cid>` rebuilds the whole journey. |
| Wire Tap | Tracing must not slow or break an order. | [service.ts:63](../shared/service.ts#L63) | Hops go out without waiting for a confirm and never throw, so a lost hop costs a trace entry and never an order. |
| Message Store | A trace kept in memory would be gone after a restart. | [server.ts:41](../observer/server.ts#L41) | The observer appends every hop to a JSONL file and reads it back at start, so the trace survives a restart. |
