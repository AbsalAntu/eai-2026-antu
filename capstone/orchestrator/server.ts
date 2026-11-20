import { startService, isBrokerUp } from "../shared/service";
import { emitHop, newId } from "../shared/trace";
import { onMessage } from "./saga";
import { publishEvent } from "./events";
import { operatorCompensate, stuckList } from "./ops";
import { pendingPublish, sagas, save } from "./state";

startService({
  name: "orchestrator",
  consume: { "saga.queue": onMessage },
  routes: (app) => {
    app.get("/sagas/stuck", (_req, res) => {
      res.status(200).json({ sagas: stuckList() });
    });
    app.post("/sagas/:correlationId/compensate", async (req, res) => {
      const result = await operatorCompensate(req.params.correlationId ?? "");
      res.status(result.status).json(result.body);
    });
  },
});

// Publishes the outcomes that could not go out when they were recorded: a saga
// marked stuck at start, or an operator compensation while the broker was away.
let flushing = false;
setInterval(async () => {
  if (flushing || pendingPublish.size === 0 || !isBrokerUp()) return;
  flushing = true;
  try {
    for (const cid of [...pendingPublish]) {
      const saga = sagas[cid];
      if (!saga?.outcome) {
        pendingPublish.delete(cid);
        continue;
      }
      if (saga.state === "needs_attention" && !saga.stuckHop) {
        const hopId = newId();
        const now = Date.now();
        emitHop({ hopId, correlationId: cid, causationId: saga.lastHop ?? null, service: "orchestrator", operation: "saga.stuck", status: "success", startedAt: now, finishedAt: now });
        saga.stuckHop = hopId;
        save();
      }
      await publishEvent(cid, saga.outcome.routingKey, saga.outcome.envelope, saga.stuckHop ?? saga.lastHop ?? null);
      pendingPublish.delete(cid);
    }
  } catch (err) {
    console.error("[orchestrator] flush failed:", (err as Error).message);
  } finally {
    flushing = false;
  }
}, 1000);
