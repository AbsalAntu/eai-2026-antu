import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import express from "express";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import { startService, publishConfirmed } from "../shared/service";
import { loadJson, saveJsonAtomic } from "../shared/store";
import { emitHop, newId } from "../shared/trace";
import { charsetOf, translateB2B, translateMobile, translateWeb, ValidationError } from "./translate";
import type { Draft } from "./translate";

const PRICING_URL = (process.env.PRICING_URL ?? "").replace(/\/+$/, "");
const PRICING_API_KEY = process.env.PRICING_API_KEY ?? "";
const TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS ?? "2500");
const ITEMS_MAX = Number(process.env.ORDER_ITEMS_MAX ?? "50");

const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);
const validateOrder = ajv.compile(JSON.parse(fs.readFileSync("/canonical/order.schema.json", "utf8")));

interface IdemRecord {
  bodySha256: string;
  state: "in_flight" | "done";
  response?: { status: number; body: unknown };
  correlationId?: string;
  createdAt: string;
}

interface OrderView {
  correlationId: string;
  businessKey: string;
  orderId: string;
  source: string;
  orderType: string;
  state: string;
  code?: string;
  duplicateOf?: string;
  updatedAt: string;
}

const idem: Record<string, IdemRecord> = loadJson("idempotency-store.json", {});
const orders: Record<string, OrderView> = loadJson("orders.json", {});

// A request that was in flight when this process died never answered its client.
for (const key of Object.keys(idem)) {
  if (idem[key]?.state === "in_flight") delete idem[key];
}
saveJsonAtomic("idempotency-store.json", idem);
saveJsonAtomic("orders.json", orders);

interface Outcome {
  status: number;
  body: unknown;
  keep: boolean;
  correlationId?: string;
  retryAfter?: number;
}

type Price =
  | { status: "ok"; unitPrice: string; currency: string; taxRate: number }
  | { status: "unknown" }
  | { status: "error" };

async function price(productId: string, correlationId: string, causationId: string): Promise<Price> {
  try {
    const res = await fetch(`${PRICING_URL}/pricing/${encodeURIComponent(productId)}`, {
      headers: {
        "X-API-Key": PRICING_API_KEY,
        "X-Correlation-Id": correlationId,
        "X-Causation-Id": causationId,
      },
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (res.status === 404) return { status: "unknown" };
    if (!res.ok) return { status: "error" };
    const body = (await res.json()) as { unitPrice: number; currency: string; taxRate: number };
    const unit = Number(body.unitPrice);
    if (!Number.isFinite(unit) || typeof body.currency !== "string") return { status: "error" };
    return { status: "ok", unitPrice: unit.toFixed(2), currency: body.currency, taxRate: Number(body.taxRate) };
  } catch {
    return { status: "error" };
  }
}

const bad = (message: string): Outcome => ({
  status: 400,
  body: { code: "validation_error", message },
  keep: false,
});

async function runPipeline(body: Buffer, isXml: boolean, contentType: string): Promise<Outcome> {
  const correlationId = randomUUID();
  const hops = { receive: newId(), translate: newId(), enrich: newId(), publish: newId() };
  const tStart = Date.now();

  let kind: "web" | "mobile" | "b2b";
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let json: any = null;
  if (isXml) {
    kind = "b2b";
  } else {
    try {
      json = JSON.parse(body.toString("utf8"));
    } catch {
      return bad("body is not valid JSON");
    }
    if (json && typeof json === "object" && json.orderId !== undefined && json.customer && typeof json.customer === "object") {
      kind = "web";
    } else if (json && typeof json === "object" && json.oid !== undefined) {
      kind = "mobile";
    } else {
      return bad("JSON body is neither a web order nor a mobile order");
    }
  }
  const tDetected = Date.now();

  let draft: Draft;
  try {
    draft = kind === "b2b" ? translateB2B(body, charsetOf(contentType)) : kind === "web" ? translateWeb(json) : translateMobile(json);
  } catch (err) {
    return bad(err instanceof ValidationError ? err.message : `cannot translate the order: ${(err as Error).message}`);
  }
  const tTranslated = Date.now();

  if (draft.lines.length === 0) return bad("the order has no items");
  if (draft.lines.length > ITEMS_MAX) return bad(`the order has ${draft.lines.length} items, at most ${ITEMS_MAX} are allowed`);

  const ids = [...new Set(draft.lines.map((l) => l.productId))];
  const prices = await Promise.all(ids.map((id) => price(id, correlationId, hops.enrich)));
  const byId = new Map(ids.map((id, i) => [id, prices[i] as Price]));
  const tEnriched = Date.now();
  if (prices.some((p) => p.status === "error")) {
    return { status: 503, body: { code: "pricing_unavailable", message: "the pricing service is not available" }, keep: false, retryAfter: 1 };
  }
  const unknown = ids.filter((id) => byId.get(id)?.status === "unknown");
  if (unknown.length > 0) {
    return { status: 422, body: { code: "unknown_product", productIds: unknown }, keep: true };
  }

  const items = draft.lines.map((l) => {
    const p = byId.get(l.productId) as Extract<Price, { status: "ok" }>;
    return {
      productId: l.productId,
      productName: l.productName,
      quantity: l.quantity,
      unitPrice: p.unitPrice,
      currency: p.currency,
      taxRate: p.taxRate,
    };
  });
  // Built from an allow-list of fields: payment details are never copied.
  const order = {
    orderId: draft.orderId,
    orderType: draft.orderType,
    source: draft.source,
    receivedAt: new Date(tStart).toISOString(),
    orderDate: draft.orderDate,
    customer: draft.customer,
    items,
    currency: draft.currency,
    status: draft.status,
  };
  if (!validateOrder(order)) {
    return bad(`the translated order is not a canonical order: ${ajv.errorsText(validateOrder.errors, { dataVar: "order" })}`);
  }

  const businessKey = `${order.source}:${order.orderId}`;
  // Stored before it is published, so a fast outcome can never be overwritten by "accepted".
  orders[correlationId] = {
    correlationId,
    businessKey,
    orderId: order.orderId,
    source: order.source,
    orderType: order.orderType,
    state: "accepted",
    updatedAt: new Date().toISOString(),
  };
  saveJsonAtomic("orders.json", orders);
  try {
    await publishConfirmed("orders.exchange", "", Buffer.from(JSON.stringify(order)), {
      persistent: true,
      contentType: "application/json",
      messageId: randomUUID(),
      headers: { correlationId, causationId: hops.publish, businessKey },
    });
  } catch {
    delete orders[correlationId];
    saveJsonAtomic("orders.json", orders);
    return { status: 503, body: { code: "broker_unavailable", message: "the broker did not confirm the order" }, keep: false, retryAfter: 1 };
  }
  const tPublished = Date.now();

  const hop = (operation: string, hopId: string, causationId: string | null, startedAt: number, finishedAt: number) =>
    emitHop({ hopId, correlationId, causationId, service: "intake", operation, status: "success", startedAt, finishedAt });
  hop("intake.receive", hops.receive, null, tStart, tDetected);
  hop("intake.translate", hops.translate, hops.receive, tDetected, tTranslated);
  hop("intake.enrich", hops.enrich, hops.translate, tTranslated, tEnriched);
  hop("intake.publish", hops.publish, hops.enrich, tEnriched, tPublished);

  return { status: 202, body: { correlationId, status: "accepted" }, keep: true, correlationId };
}

startService({
  name: "intake",
  consume: {},
  routes: (app) => {
    app.post("/orders", express.raw({ type: () => true, limit: "2mb" }), async (req, res) => {
      const send = (o: Outcome) => {
        if (o.retryAfter) res.set("Retry-After", String(o.retryAfter));
        res.status(o.status).json(o.body);
      };

      const key = req.header("Idempotency-Key");
      if (!key || !/^[A-Za-z0-9._:-]{1,128}$/.test(key)) {
        send(bad("the Idempotency-Key header is required: 1 to 128 characters from A-Z a-z 0-9 . _ : -"));
        return;
      }
      const contentType = String(req.header("Content-Type") ?? "");
      const media = (contentType.split(";")[0] ?? "").trim().toLowerCase();
      const isXml = media === "application/xml" || media === "text/xml";
      if (!isXml && media !== "application/json") {
        send({ status: 415, body: { code: "unsupported_media_type", message: "use application/json or application/xml" }, keep: false });
        return;
      }

      const body = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
      const sha = createHash("sha256").update(body).digest("hex");
      const existing = idem[key];
      if (existing) {
        if (existing.bodySha256 !== sha) {
          send({ status: 409, body: { code: "idempotency_payload_mismatch", message: "this Idempotency-Key was used with a different body" }, keep: false });
        } else if (existing.state === "in_flight" || !existing.response) {
          send({ status: 409, body: { code: "idempotency_conflict", message: "a request with this Idempotency-Key is still in progress" }, keep: false });
        } else {
          res.status(existing.response.status).json(existing.response.body);
        }
        return;
      }

      // No await between the check above and this write: two requests cannot both pass.
      idem[key] = { bodySha256: sha, state: "in_flight", createdAt: new Date().toISOString() };
      saveJsonAtomic("idempotency-store.json", idem);

      let outcome: Outcome;
      try {
        outcome = await runPipeline(body, isXml, contentType);
      } catch (err) {
        console.error("[intake] unexpected error", err);
        outcome = { status: 500, body: { code: "internal_error", message: "unexpected error" }, keep: false };
      }
      if (outcome.keep) {
        idem[key] = {
          bodySha256: sha,
          state: "done",
          response: { status: outcome.status, body: outcome.body },
          ...(outcome.correlationId ? { correlationId: outcome.correlationId } : {}),
          createdAt: (idem[key] as IdemRecord).createdAt,
        };
      } else {
        delete idem[key];
      }
      saveJsonAtomic("idempotency-store.json", idem);
      send(outcome);
    });

    app.get("/orders/:correlationId", (req, res) => {
      const view = orders[req.params.correlationId ?? ""];
      if (!view) {
        res.status(404).json({ code: "not_found", message: "no order with this correlationId" });
        return;
      }
      res.status(200).json(view);
    });
  },
});
