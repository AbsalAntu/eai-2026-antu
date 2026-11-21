const PAYMENT = process.env.PAYMENT_URL ?? "";
const INVENTORY = process.env.INVENTORY_URL ?? "";
const SHIPPING = process.env.SHIPPING_URL ?? "";
const NOTIFICATION = process.env.NOTIFICATION_URL ?? "";
const TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS ?? "2500");

export interface Order {
  orderId: string;
  orderType: string;
  source: string;
  customer: { email: string };
  items: Array<{ unitPrice: string; quantity: number }>;
}

export interface CallResult {
  ok: boolean;
  /** a 4xx: it will say no again */
  permanent: boolean;
  status: "success" | "failed" | "timeout";
  code: string;
  http: number;
}

function cents(price: string): number {
  const m = /^(-?)(\d+)\.(\d{2})$/.exec(price);
  if (!m) throw new Error(`bad unitPrice: ${price}`);
  const value = Number(m[2]) * 100 + Number(m[3]);
  return m[1] ? -value : value;
}

/** Whole cents as integers, formatted once at the end: "63.90", never a float. */
export function amountOf(items: Array<{ unitPrice: string; quantity: number }>): string {
  let total = 0;
  for (const item of items) total += cents(item.unitPrice) * item.quantity;
  const sign = total < 0 ? "-" : "";
  const abs = Math.abs(total);
  return `${sign}${Math.floor(abs / 100)}.${String(abs % 100).padStart(2, "0")}`;
}

export function planFor(orderType: string): string[] {
  // b2b is invoiced: no payment. Route on orderType, never on source.
  return orderType === "b2b"
    ? ["inventory", "shipping", "notification"]
    : ["payment", "inventory", "shipping", "notification"];
}

async function post(url: string, body: unknown, cid: string, hop: string, key?: string): Promise<CallResult> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "X-Correlation-Id": cid,
    "X-Causation-Id": hop,
  };
  if (key) headers["Idempotency-Key"] = key;
  try {
    const res = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (res.ok) return { ok: true, permanent: false, status: "success", code: "", http: res.status };
    let code = "";
    try {
      const parsed = (await res.json()) as { code?: unknown };
      if (typeof parsed?.code === "string") code = parsed.code;
    } catch {
      // not JSON: keep the default
    }
    const permanent = res.status >= 400 && res.status < 500;
    return {
      ok: false,
      permanent,
      status: "failed",
      code: code || (permanent ? "request_rejected" : "service_unavailable"),
      http: res.status,
    };
  } catch (err) {
    const name = (err as Error).name;
    if (name === "TimeoutError" || name === "AbortError") {
      return { ok: false, permanent: false, status: "timeout", code: "timeout", http: 0 };
    }
    return { ok: false, permanent: false, status: "failed", code: "unreachable", http: 0 };
  }
}

export function callForward(step: string, order: Order, bk: string, cid: string, hop: string): Promise<CallResult> {
  switch (step) {
    case "payment":
      return post(`${PAYMENT}/payment/authorize`, { orderId: order.orderId, amount: amountOf(order.items) }, cid, hop, `authorize:${bk}`);
    case "inventory":
      return post(`${INVENTORY}/inventory/reserve`, { orderId: order.orderId, items: order.items }, cid, hop, `reserve:${bk}`);
    case "shipping":
      return post(`${SHIPPING}/shipping/create`, { orderId: order.orderId }, cid, hop);
    default:
      return post(`${NOTIFICATION}/notification/send`, { orderId: order.orderId, recipient: order.customer.email }, cid, hop);
  }
}

export function callUndo(undo: string, orderId: string, bk: string, cid: string, hop: string): Promise<CallResult> {
  if (undo === "payment_refund") return post(`${PAYMENT}/payment/refund`, { orderId }, cid, hop, `refund:${bk}`);
  return post(`${INVENTORY}/inventory/release`, { orderId }, cid, hop, `release:${bk}`);
}
