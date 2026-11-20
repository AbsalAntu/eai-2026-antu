import { startService, ackOnly } from "../shared/service";

startService({ name: "observer", consume: { "trace.store": ackOnly } });
