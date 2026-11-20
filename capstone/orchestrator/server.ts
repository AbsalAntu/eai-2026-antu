import { startService, ackOnly } from "../shared/service";

startService({ name: "orchestrator", consume: { "saga.queue": ackOnly } });
