import { startService, ackOnly } from "../shared/service";

startService({ name: "notifier", consume: { "notifications.queue": ackOnly } });
