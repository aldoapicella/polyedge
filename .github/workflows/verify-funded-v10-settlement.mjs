import { closeSync, constants, fstatSync, openSync, readFileSync } from "node:fs";
import { TextDecoder } from "node:util";
import { validatedDurableInternalSettlementAccounting } from
  "../../venue-probe/src/compounding-risk.mjs";

const [file, blobName] = process.argv.slice(2);
let descriptor;

try {
  if (!file || !blobName) throw new Error("missing input");
  descriptor = openSync(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  const stat = fstatSync(descriptor);
  if (!stat.isFile() || stat.nlink !== 1 ||
      stat.size <= 0 || stat.size > 16_384) {
    throw new Error("unsafe input");
  }
  const bytes = readFileSync(descriptor);
  if (bytes.length !== stat.size) throw new Error("input changed");
  const value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  const accounting = validatedDurableInternalSettlementAccounting(
    value,
    "dynamic-quote-funded-2026-08-13-v10",
    blobName
  );
  process.stdout.write(`${JSON.stringify(accounting)}\n`);
} catch {
  console.error("funded v10 internal settlement validation failed");
  process.exitCode = 1;
} finally {
  if (descriptor !== undefined) closeSync(descriptor);
}
