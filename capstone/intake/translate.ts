import { XMLParser } from "fast-xml-parser";

export class ValidationError extends Error {}

export interface Line {
  productId: string;
  productName: string;
  quantity: number;
}

export interface Draft {
  orderId: string;
  orderType: string;
  source: "web" | "mobile" | "b2b";
  orderDate: string;
  customer: { name: string; email: string; address: { street: string; city: string; postalCode: string; country: string } };
  currency: string;
  status: string;
  lines: Line[];
}

const STATUSES = ["new", "processing", "shipped", "delivered"];

function need<T>(value: T | undefined | null, what: string): T {
  if (value === undefined || value === null || (value as unknown) === "") {
    throw new ValidationError(`missing field: ${what}`);
  }
  return value;
}

function toIso(value: unknown, what: string): string {
  const d = new Date(String(value));
  if (Number.isNaN(d.getTime())) throw new ValidationError(`invalid date: ${what}`);
  return d.toISOString();
}

export function normalizeProductId(source: "web" | "mobile" | "b2b", rawId: string): string {
  if (source === "mobile") return rawId.startsWith("PROD-") ? rawId : `PROD-${rawId}`;
  if (source === "b2b") return rawId.replace(/^SKU-/, "");
  return rawId;
}

function mapMobileStatus(code: number): string {
  const status = ({ 1: "new", 2: "processing", 3: "shipped", 4: "delivered" } as Record<number, string>)[code];
  if (!status) throw new ValidationError(`unknown mobile status code: ${code}`);
  return status;
}

function mapB2BStatus(raw: string): string {
  const status = raw.trim().toLowerCase();
  if (!STATUSES.includes(status)) throw new ValidationError(`unknown B2B status: ${raw}`);
  return status;
}

function mapCurrencyCode(code: number): string {
  const currency = ({ 978: "EUR", 840: "USD", 826: "GBP" } as Record<number, string>)[code];
  if (!currency) throw new ValidationError(`unknown numeric currency code: ${code}`);
  return currency;
}

function parseMobileAddress(addr: string): Draft["customer"]["address"] {
  const parts = addr.split(", ").map((x) => x.trim());
  if (parts.length < 4) throw new ValidationError(`cannot parse mobile address: ${addr}`);
  const n = parts.length;
  return {
    street: parts.slice(0, n - 3).join(", "),
    city: parts[n - 3] as string,
    postalCode: parts[n - 2] as string,
    country: parts[n - 1] as string,
  };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Json = any;

export function translateWeb(input: Json): Draft {
  const customer = need(input.customer, "customer");
  const address = need(customer.address, "customer.address");
  if (!Array.isArray(input.items)) throw new ValidationError("missing field: items");
  return {
    orderId: String(need(input.orderId, "orderId")),
    orderType: String(need(input.orderType, "orderType")),
    source: "web",
    orderDate: toIso(need(input.orderDate, "orderDate"), "orderDate"),
    customer: {
      name: String(need(customer.name, "customer.name")),
      email: String(need(customer.email, "customer.email")),
      address: {
        street: String(need(address.street, "address.street")),
        city: String(need(address.city, "address.city")),
        postalCode: String(need(address.postalCode, "address.postalCode")),
        country: String(need(address.country, "address.country")),
      },
    },
    currency: String(need(input.currency, "currency")),
    status: String(need(input.status, "status")),
    lines: input.items.map((i: Json) => ({
      productId: normalizeProductId("web", String(need(i?.productId, "items.productId"))),
      productName: String(need(i?.productName, "items.productName")),
      quantity: Number(need(i?.quantity, "items.quantity")),
    })),
  };
}

export function translateMobile(input: Json): Draft {
  if (!Array.isArray(input.items)) throw new ValidationError("missing field: items");
  const ts = Number(need(input.ts, "ts"));
  if (!Number.isFinite(ts)) throw new ValidationError("invalid field: ts");
  return {
    orderId: String(need(input.oid, "oid")),
    orderType: String(need(input.ot, "ot")),
    source: "mobile",
    orderDate: new Date(ts * 1000).toISOString(),
    customer: {
      name: String(need(input.cust_name, "cust_name")),
      email: String(need(input.cust_email, "cust_email")),
      address: parseMobileAddress(String(need(input.addr, "addr"))),
    },
    currency: mapCurrencyCode(Number(need(input.cur, "cur"))),
    status: mapMobileStatus(Number(need(input.st, "st"))),
    lines: input.items.map((i: Json) => ({
      productId: normalizeProductId("mobile", String(need(i?.pid, "items.pid"))),
      productName: String(need(i?.pname, "items.pname")),
      quantity: Number(need(i?.qty, "items.qty")),
    })),
  };
}

export function charsetOf(contentType: string): string | undefined {
  return /charset\s*=\s*"?([^";\s]+)/i.exec(contentType)?.[1];
}

/** A charset on the Content-Type wins (RFC 7303); otherwise the encoding in the XML declaration. */
export function decodeXml(bytes: Buffer, charset?: string): string {
  let label = charset;
  if (!label) {
    const head = bytes.subarray(0, 200).toString("latin1");
    label = /<\?xml[^>]*encoding\s*=\s*["']([^"']+)["']/i.exec(head)?.[1] ?? "utf-8";
  }
  try {
    return new TextDecoder(label).decode(bytes);
  } catch {
    throw new ValidationError(`unsupported encoding: ${label}`);
  }
}

const xmlParser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_", parseTagValue: false });

export function translateB2B(bytes: Buffer, charset?: string): Draft {
  const parsed = xmlParser.parse(decodeXml(bytes, charset)) as Json;
  const po = need(parsed?.PurchaseOrder, "PurchaseOrder");
  const buyer = need(po.BuyerParty, "BuyerParty");
  const ship = need(buyer.ShipToAddress, "ShipToAddress");
  const lineItems = need(po.LineItems, "LineItems");
  const raw = need(lineItems.LineItem, "LineItem");
  const items: Json[] = Array.isArray(raw) ? raw : [raw];
  return {
    orderId: String(need(po["@_orderId"], "orderId")),
    orderType: String(need(po["@_orderType"], "orderType")),
    source: "b2b",
    orderDate: toIso(need(po["@_orderDate"], "orderDate"), "orderDate"),
    customer: {
      name: String(need(buyer.Name, "BuyerParty.Name")),
      email: String(need(buyer.ContactEmail, "BuyerParty.ContactEmail")),
      address: {
        street: String(need(ship.Street, "ShipToAddress.Street")),
        city: String(need(ship.City, "ShipToAddress.City")),
        postalCode: String(need(ship.PostalCode, "ShipToAddress.PostalCode")),
        country: String(need(ship["@_country"], "ShipToAddress.country")),
      },
    },
    currency: String(need(lineItems["@_currency"], "LineItems.currency")),
    status: mapB2BStatus(String(need(po.Status, "Status"))),
    // UnitListPrice, ContractDate, Notes and PaymentDetails are never read.
    lines: items.map((li) => ({
      productId: normalizeProductId("b2b", String(need(li["@_sku"], "LineItem.sku"))),
      productName: String(li.Description ?? ""),
      quantity: Number(need(li["@_quantity"], "LineItem.quantity")),
    })),
  };
}
