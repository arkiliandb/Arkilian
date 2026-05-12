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

await db.close();

console.log("\nAll tests passed!");
