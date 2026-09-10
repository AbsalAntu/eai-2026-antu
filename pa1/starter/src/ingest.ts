/**
 * PA1 — legacy file ingestion.
 */

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export interface Order {
  orderId: string;
  customerId: string;
  customerName: string;
  orderDate: string;
  amount: string;
  currency: string;
}

export interface RejectedRecord {
  line: number;
  raw: string;
  reason: string;
}

export interface Report {
  orders: Order[];
  rejected: RejectedRecord[];
  unmatchedCustomers: string[];
}

export interface IngestOptions {
  ordersPath: string;
  customersPath: string;
}

export const ORDER_LAYOUT = {
  orderId: [0, 10],
  customerId: [10, 20],
  customerName: [20, 52],
  orderDate: [52, 62],
  amount: [62, 74],
  currency: [74, 77],
} as const;

export const ORDER_LINE_LENGTH = 77;

const PA1_ROOT = fileURLToPath(new URL("../../", import.meta.url));

export const DEFAULT_ORDERS_PATH = path.join(PA1_ROOT, "data", "orders-20260901.txt");
export const DEFAULT_CUSTOMERS_PATH = path.join(PA1_ROOT, "data", "customers.csv");
export const OUTPUT_PATH = path.join(PA1_ROOT, "out", "report.json");

export function decodeOrderFile(bytes: Buffer): string {
  const decoder = new TextDecoder("windows-1257");
  return decoder.decode(bytes);
}

export function toIsoDate(ddmmyyyy: string): string {
  const day = ddmmyyyy.slice(0, 2);
  const month = ddmmyyyy.slice(3, 5);
  const year = ddmmyyyy.slice(6, 10);
  return `${year}-${month}-${day}`;
}

export function toDecimalString(amount: string): string {
  return amount.trim().replace(",", ".");
}

export function parseCustomers(csv: string): Map<string, string> {
  const lines = csv.split(/\r?\n/).filter((line) => line.length > 0);
  const map = new Map<string, string>();
  for (let i = 1; i < lines.length; i++) {
    const rawLine = lines[i];
    if (!rawLine) continue;
    const parts = rawLine.split(";");
    const customerId = parts[0];
    const fullName = parts[1];
    if (customerId === undefined || fullName === undefined) continue;
    map.set(customerId, fullName);
  }
  return map;
}

export function ingest(options: IngestOptions): Report {
  const orderBytes = readFileSync(options.ordersPath);
  const orderText = decodeOrderFile(orderBytes);
  const customerText = readFileSync(options.customersPath, "utf8");
  const customers = parseCustomers(customerText);

  const lines = orderText.split(/\r?\n/).filter((line) => line.length > 0);

  const orders: Order[] = [];
  const rejected: RejectedRecord[] = [];
  const seenCustomerIds = new Set<string>();

  lines.forEach((line, index) => {
    const lineNumber = index + 1;

    if (line.length !== ORDER_LINE_LENGTH) {
      rejected.push({
        line: lineNumber,
        raw: line,
        reason: `expected ${ORDER_LINE_LENGTH} characters, got ${line.length}`,
      });
      return;
    }

    const [oiStart, oiEnd] = ORDER_LAYOUT.orderId;
    const [ciStart, ciEnd] = ORDER_LAYOUT.customerId;
    const [cnStart, cnEnd] = ORDER_LAYOUT.customerName;
    const [odStart, odEnd] = ORDER_LAYOUT.orderDate;
    const [amStart, amEnd] = ORDER_LAYOUT.amount;
    const [curStart, curEnd] = ORDER_LAYOUT.currency;

    const orderId = line.slice(oiStart, oiEnd).trim();
    const customerId = line.slice(ciStart, ciEnd).trim();
    const customerName = line.slice(cnStart, cnEnd).trim();
    const orderDateRaw = line.slice(odStart, odEnd).trim();
    const amountRaw = line.slice(amStart, amEnd).trim();
    const currency = line.slice(curStart, curEnd).trim();

    orders.push({
      orderId,
      customerId,
      customerName,
      orderDate: toIsoDate(orderDateRaw),
      amount: toDecimalString(amountRaw),
      currency,
    });

    seenCustomerIds.add(customerId);
  });

  const unmatchedCustomers: string[] = [];
  for (const customerId of customers.keys()) {
    if (!seenCustomerIds.has(customerId)) {
      unmatchedCustomers.push(customerId);
    }
  }

  return { orders, rejected, unmatchedCustomers };
}

export function main(): void {
  const report = ingest({
    ordersPath: DEFAULT_ORDERS_PATH,
    customersPath: DEFAULT_CUSTOMERS_PATH,
  });

  mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  writeFileSync(OUTPUT_PATH, JSON.stringify(report, null, 2) + "\n", "utf8");

  console.log(
    `wrote ${OUTPUT_PATH}\n` +
      `  ${report.orders.length} orders\n` +
      `  ${report.rejected.length} rejected\n` +
      `  ${report.unmatchedCustomers.length} customers with no order`,
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main();
}