import Arkilian from "./index.js";
import { join } from "path";

import { fileURLToPath } from "url";

// Recreate __dirname functionality for ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = join(__filename, "..");

console.log("Testing Arkilian Node.js bindings...\n");

const dbPath = join(__dirname, "test.db");
const db = new Arkilian("dummy-test-token-00000000-0000-0000-0000-000000000000", dbPath);

console.log("1. Drop old table if exists and recreate...");
await db.exec("DROP TABLE IF EXISTS users");
await db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");
console.log("   OK");

console.log("2. Insert data with run()...");
await db.run("INSERT INTO users (name, email) VALUES (?, ?)", [
  "Alice",
  "alice@example.com",
]);
await db.run("INSERT INTO users (name, email) VALUES (?, ?)", [
  "Bob",
  "bob@example.com",
]);
console.log("   OK");

console.log("3. Query data with all()...");
const users = await db.all("SELECT * FROM users");
console.log(`   Found ${users.length} users:`);
users.forEach((u) =>
  console.log(`   - id: ${u.id}, name: ${u.name}, email: ${u.email}`),
);

console.log("\n4. Query with params...");
const alice = await db.all("SELECT * FROM users WHERE name = ?", ["Alice"]);
console.log(`   Found ${alice.length} Alice(s)`);

console.log("\n5. Update data...");
await db.run("UPDATE users SET email = ? WHERE name = ?", [
  "bob@newdomain.com",
  "Bob",
]);

console.log("\n6. Verify update...");
const bob = await db.all("SELECT * FROM users WHERE name = ?", ["Bob"]);
console.log(`   Bob's email: ${bob[0].email}`);

console.log("\n7. Delete data...");
await db.run("DELETE FROM users WHERE name = ?", ["Alice"]);
const remaining = await db.all("SELECT * FROM users");
console.log(`   Remaining users: ${remaining.length}`);

console.log("\n8. Multi-statement test (no statement loss)...");
// Prepare two statements — both should remain accessible
db.prepare("SELECT * FROM users WHERE name = ?");
const stmtIdx0 = db.stmtCount() - 1;
db.prepare("SELECT COUNT(*) as cnt FROM users");
const stmtIdx1 = db.stmtCount() - 1;
console.log(`   Statement count: ${db.stmtCount()}`);

// Step the second (current) statement
const countResult = db.step();
console.log(
  `   COUNT step result: ${countResult} (expected ${Arkilian.SQLITE_ROW})`,
);
const cnt = db.get(0);
console.log(`   User count: ${cnt}`);
await db.finalize();

// Switch back to first statement — it should still be alive
db.useStmt(stmtIdx0);
db.bindText(1, "Bob");
const bobStep = db.step();
console.log(`   Bob step result: ${bobStep} (expected ${Arkilian.SQLITE_ROW})`);
const bobName = db.get(1);
console.log(`   Got Bob's name: ${bobName}`);
await db.finalize();
console.log("   OK - statements not lost");

console.log("\n9. BigInt bind round-trip (regression)...");
await db.exec("DROP TABLE IF EXISTS bigints");
await db.exec("CREATE TABLE bigints (v INTEGER)");
const big = 1718400000123456789n; // > 2^53, must survive exactly
await db.run("INSERT INTO bigints (v) VALUES (?)", [big]);
const bigRow = await db.all("SELECT v FROM bigints");
console.log(`   bigint round-trip: ${bigRow[0].v} (${typeof bigRow[0].v})`);
if (typeof bigRow[0].v !== "bigint" || bigRow[0].v !== big) {
  console.error("   FAIL: bigint did not round-trip exactly");
  process.exit(1);
}
await db.exec("DROP TABLE bigints");
console.log("   OK");

console.log("\n10. Worker-thread concurrent cursor use (regression)...");
const { Worker } = await import("worker_threads");
const workerCode = `
  const { parentPort } = require("worker_threads");
  const Arkilian = require(${JSON.stringify(join(__dirname, "index.js"))}).default;
  const db = new Arkilian("dummy-test-token-00000000-0000-0000-0000-000000000000",
    ${JSON.stringify(dbPath)});
  try {
    for (let i = 0; i < 500; i++) {
      db.exec("INSERT INTO thrash (v) VALUES (" + i + ")");
      const rows = db.all("SELECT COUNT(*) AS c FROM thrash");
      if (!rows || rows[0].c === undefined) throw new Error("bad result");
    }
    parentPort.postMessage("ok");
  } catch (e) {
    parentPort.postMessage("fail: " + e.message);
  } finally {
    db.close();
  }
`;
await db.exec("DROP TABLE IF EXISTS thrash");
await db.exec("CREATE TABLE thrash (v INTEGER)");
const workers = [];
for (let w = 0; w < 4; w++) {
  workers.push(
    new Promise((resolve) => {
      const worker = new Worker(workerCode, { eval: true });
      worker.on("message", (m) => {
        if (m !== "ok") {
          console.error(`   worker ${w} FAIL: ${m}`);
          process.exit(1);
        }
        resolve();
      });
      worker.on("error", (e) => {
        console.error(`   worker ${w} crashed: ${e.message}`);
        process.exit(1);
      });
    }),
  );
}
await Promise.all(workers);
const thrashCount = await db.all("SELECT COUNT(*) AS c FROM thrash");
if (thrashCount[0].c !== 2000) {
  console.error(`   FAIL: expected 2000 rows, got ${thrashCount[0].c}`);
  process.exit(1);
}
console.log("   4 workers x 500 concurrent ops: all rows intact (2000)");
await db.exec("DROP TABLE thrash");
console.log("   OK");

await db.close();
console.log("\nAll tests passed!");
