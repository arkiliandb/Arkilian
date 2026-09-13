#!/usr/bin/env python3
"""
Arkilian Python SDK - Simple Quickstart Example

Demonstrates:
1. Connecting to an embedded Arkilian database
2. Using Python context managers (with Arkilian(...) as db)
3. Schema definition (DDL)
4. Parameterized inserts and queries
5. ACID transactions with rollback / commit
6. Clean resource disposal
"""

import os
import sys

# Automatically locate libarkilian.dylib/.so if ARKILIAN_LIB_PATH is not set
if "ARKILIAN_LIB_PATH" not in os.environ:
    candidate = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "c", "lib", "libarkilian.dylib"))
    if not os.path.exists(candidate):
        candidate = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "c", "lib", "libarkilian.so"))
    if os.path.exists(candidate):
        os.environ["ARKILIAN_LIB_PATH"] = candidate

# Embedded-only mode for quickstart (disable sidecar destination warnings)
os.environ["ARKILIAN_ENABLE_BACKUP"] = "0"

from arkilian import Arkilian

DB_PATH = "quickstart.sqlite"


def cleanup_db(path):
    for ext in ["", "-wal", "-shm"]:
        target = path + ext
        if os.path.exists(target):
            os.remove(target)


def main():
    print("=== Arkilian Python SDK: Simple Quickstart ===\n")
    cleanup_db(DB_PATH)

    print(f"[1] Opening Arkilian database at '{DB_PATH}'...")
    with Arkilian(DB_PATH) as db:
        # 1. Create table schema
        print("[2] Creating schema...")
        db.exec("""
            CREATE TABLE IF NOT EXISTS inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 0
            );
        """)
        print("    ✓ Table 'inventory' created.")

        # 2. Insert records using parameterized queries
        print("[3] Inserting sample inventory items...")
        insert_sql = "INSERT INTO inventory (sku, name, price, quantity) VALUES (?, ?, ?, ?)"
        db.run(insert_sql, ["SKU-101", "Mechanical Keyboard", 129.99, 45])
        db.run(insert_sql, ["SKU-102", "Ergonomic Mouse", 69.50, 120])
        db.run(insert_sql, ["SKU-103", "4K Ultra-Wide Monitor", 449.00, 18])
        print("    ✓ 3 inventory records inserted.")

        # 3. Query all items
        print("\n[4] Querying all inventory items:")
        items = db.all("SELECT id, sku, name, price, quantity FROM inventory ORDER BY id ASC")
        for item in items:
            print(f"    - ID {item['id']}: {item['name']} ({item['sku']}) | Price: ${item['price']:.2f} | In Stock: {item['quantity']}")

        # 4. Query single item by SKU
        print("\n[5] Querying item by SKU ('SKU-102')...")
        single = db.all("SELECT * FROM inventory WHERE sku = ?", ["SKU-102"])[0]
        print(f"    Found: {single['name']} (Stock: {single['quantity']}, Price: ${single['price']:.2f})")

        # 5. Atomic transaction demonstration
        print("\n[6] Executing atomic stock update transaction...")
        try:
            db.begin()
            db.run("UPDATE inventory SET quantity = quantity - 5 WHERE sku = ?", ["SKU-101"])
            db.run("UPDATE inventory SET quantity = quantity + 50 WHERE sku = ?", ["SKU-103"])
            db.commit()
            print("    ✓ Transaction committed successfully.")
        except Exception as e:
            db.rollback()
            print(f"    ✗ Transaction rolled back: {e}")

        updated_items = db.all("SELECT sku, name, quantity FROM inventory WHERE sku IN (?, ?)", ["SKU-101", "SKU-103"])
        print("    Updated Stock Levels:")
        for item in updated_items:
            print(f"    - {item['name']} ({item['sku']}): {item['quantity']} units")

    # Context manager automatically closes database
    print("\n[7] Database closed safely by context manager.")
    print("\n=== Quickstart Completed Successfully ===")


if __name__ == "__main__":
    main()
