import fs from "node:fs";
import path from "node:path";

export const DATA_DIR = process.env.DATA_DIR ?? "/data";

export function loadJson<T>(file: string, initial: T): T {
  try {
    return JSON.parse(fs.readFileSync(path.join(DATA_DIR, file), "utf8")) as T;
  } catch {
    return initial;
  }
}

/** Write a temporary file in the same folder, then rename it over the old one (R-3.5.3). */
export function saveJsonAtomic(file: string, value: unknown): void {
  const full = path.join(DATA_DIR, file);
  fs.mkdirSync(DATA_DIR, { recursive: true });
  const tmp = `${full}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(value, null, 2));
  fs.renameSync(tmp, full);
}
