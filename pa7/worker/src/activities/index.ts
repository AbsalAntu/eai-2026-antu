import { ApplicationFailure } from "@temporalio/activity";
import type { CanonicalItem } from "../types";

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

// docker-compose.yml sets these. Never hardcode a downstream URL.
const PAYMENT_URL = requiredEnv("PAYMENT_URL");
const INVENTORY_URL = requiredEnv("INVENTORY_URL");
const SHIPPING_URL = requiredEnv("SHIPPING_URL");
const NOTIFICATION_URL = requiredEnv("NOTIFICATION_URL");

/**
 * One POST. 2xx returns. 4xx is a business refusal and will refuse again, so
 * it is non-retryable and carries the mock's own code. 5xx or no answer is
 * temporary, so it is retryable.
 */
async function post(url: string, body: unknown): Promise<void> {
  let res: Response;
  try {
    res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (err) {
    throw ApplicationFailure.retryable(`No answer from ${url}: ${(err as Error).message}`, "unreachable");
  }
  if (res.ok) return;

  let code = "request_failed";
  let message = `${url} answered HTTP ${res.status}`;
  try {
    const parsed = (await res.json()) as { code?: unknown; message?: unknown };
    if (typeof parsed?.code === "string") code = parsed.code;
    if (typeof parsed?.message === "string") message = parsed.message;
  } catch {
    // body was not JSON: keep the defaults
  }

  if (res.status >= 400 && res.status < 500) {
    throw ApplicationFailure.nonRetryable(message, code);
  }
  throw ApplicationFailure.retryable(message, code);
}

/** POST {PAYMENT_URL}/payment/authorize with { orderId, amount }. */
export async function authorizePayment(orderId: string, amount: string): Promise<void> {
  await post(`${PAYMENT_URL}/payment/authorize`, { orderId, amount });
}

/** POST {INVENTORY_URL}/inventory/reserve with { orderId, items }. */
export async function reserveInventory(orderId: string, items: CanonicalItem[]): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/reserve`, { orderId, items });
}

/** POST {SHIPPING_URL}/shipping/create with { orderId }. */
export async function createShipment(orderId: string): Promise<void> {
  await post(`${SHIPPING_URL}/shipping/create`, { orderId });
}

/** POST {NOTIFICATION_URL}/notification/send with { orderId, recipient }. */
export async function sendNotification(orderId: string, recipient: string): Promise<void> {
  await post(`${NOTIFICATION_URL}/notification/send`, { orderId, recipient });
}

/** Compensates authorizePayment: POST {PAYMENT_URL}/payment/refund with { orderId }. */
export async function refundPayment(orderId: string): Promise<void> {
  await post(`${PAYMENT_URL}/payment/refund`, { orderId });
}

/** Compensates reserveInventory: POST {INVENTORY_URL}/inventory/release with { orderId }. */
export async function releaseInventory(orderId: string): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/release`, { orderId });
}
