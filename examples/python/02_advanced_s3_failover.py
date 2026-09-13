#!/usr/bin/env python3
"""
Arkilian Python SDK - Advanced S3 Streaming & Cold-Start Disaster Recovery

Demonstrates:
1. Real-time WAL streaming to MinIO / AWS S3
2. Sidecar telemetry inspection (health, health flags, outbox queue depth)
3. Zero-lag WAL flushes via db.wal_flush()
4. Simulating complete host catastrophe (local file deletion)
5. Instant disaster recovery using Arkilian.hydrate_s3()
6. Data parity and integrity validation on the hydrated database
"""

import os
import sys
import time

# Automatically locate libarkilian.dylib/.so if ARKILIAN_LIB_PATH is not set
if "ARKILIAN_LIB_PATH" not in os.environ:
    candidate = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "c", "lib", "libarkilian.dylib"))
    if not os.path.exists(candidate):
        candidate = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "c", "lib", "libarkilian.so"))
    if os.path.exists(candidate):
        os.environ["ARKILIAN_LIB_PATH"] = candidate

S3_CONFIG = {
    "endpoint": os.environ.get("ARKILIAN_S3_ENDPOINT", "http://127.0.0.1:9000"),
    "bucket": os.environ.get("ARKILIAN_S3_BUCKET", "arkilian-test-bucket"),
    "region": os.environ.get("ARKILIAN_S3_REGION", "us-east-1"),
    "access_key": os.environ.get("ARKILIAN_S3_ACCESS_KEY", "minioadmin"),
    "secret_key": os.environ.get("ARKILIAN_S3_SECRET_KEY", "minioadmin"),
    "prefix": os.environ.get("ARKILIAN_S3_PREFIX", "python-demo"),
}

# Configure environment for the Arkilian sidecar worker threads
os.environ["ARKILIAN_ENABLE_BACKUP"] = "1"
os.environ["ARKILIAN_S3_ENDPOINT"] = S3_CONFIG["endpoint"]
os.environ["ARKILIAN_S3_BUCKET"] = S3_CONFIG["bucket"]
os.environ["ARKILIAN_S3_REGION"] = S3_CONFIG["region"]
os.environ["ARKILIAN_S3_ACCESS_KEY"] = S3_CONFIG["access_key"]
os.environ["ARKILIAN_S3_SECRET_KEY"] = S3_CONFIG["secret_key"]
os.environ["ARKILIAN_S3_PREFIX"] = S3_CONFIG["prefix"]
os.environ["ARKILIAN_MANIFEST_HMAC_KEY"] = "super-secret-hmac-key-for-manifests"
os.environ["ARKILIAN_CHUNK_INTERVAL_SEC"] = "1"
os.environ["ARKILIAN_MANIFEST_INTERVAL_SEC"] = "1"

from arkilian import Arkilian

PRIMARY_DB = "primary_production.sqlite"
RESTORED_DB = "hydrated_recovery.sqlite"


def cleanup_db(path):
    for ext in ["", "-wal", "-shm"]:
        target = path + ext
        if os.path.exists(target):
            os.remove(target)


def main():
    print("=== Arkilian Python SDK: Advanced S3 Streaming & Cold-Start Hydration ===\n")
    print(f"[Config] Target S3 Endpoint: {S3_CONFIG['endpoint']}")
    print(f"[Config] Bucket: {S3_CONFIG['bucket']} | Prefix: {S3_CONFIG['prefix']}\n")

    cleanup_db(PRIMARY_DB)
    cleanup_db(RESTORED_DB)

    # -------------------------------------------------------------
    # Phase 1: Primary Database Workload with Continuous S3 Streaming
    # -------------------------------------------------------------
    print("[Phase 1] Opening Primary Node with S3 WAL Streaming...")
    db = Arkilian(PRIMARY_DB)

    print(f"  Telemetry -> Sidecar Healthy: {db.is_healthy}")
    print(f"  Telemetry -> Health Flags: 0x{db.backup_health_flags:x}")
    print(f"  Telemetry -> Queue Depth: {db.backup_queue_depth}")

    print("\n[Phase 1] Creating schema and streaming order audit log...")
    db.exec("""
        CREATE TABLE IF NOT EXISTS audit_orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id TEXT NOT NULL,
            total_amount REAL NOT NULL,
            status TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    """)

    total_records = 25
    insert_sql = "INSERT INTO audit_orders (customer_id, total_amount, status) VALUES (?, ?, ?)"

    db.begin()
    for i in range(1, total_records + 1):
        amt = 25.0 + (i * 7.75)
        status = "COMPLETED" if i % 2 == 0 else "PENDING"
        db.run(insert_sql, [f"cust_{i % 8}", round(amt, 2), status])
    db.commit()
    print(f"  ✓ Committed {total_records} audit log entries in an atomic transaction.")

    # Explicit WAL flush: flush buffer to outbox
    print("\n[Phase 1] Triggering explicit WAL flush to S3 sidecar...")
    db.wal_flush()

    print("  Waiting for S3 sidecar outbox to drain...")
    for _ in range(20):
        time.sleep(0.5)
        if db.backup_queue_depth == 0:
            break
    # Allow manifest upload to finalize
    time.sleep(1.5)

    print(f"  Telemetry -> Queue Depth: {db.backup_queue_depth}")
    print(f"  Telemetry -> Sidecar Healthy: {db.is_healthy}")

    primary_count = db.all("SELECT COUNT(*) as cnt FROM audit_orders")[0]["cnt"]
    print(f"  Primary DB confirmed record count: {primary_count}")

    print("[Phase 1] Closing primary database connection.")
    db.close()

    # -------------------------------------------------------------
    # Phase 2: Disaster Simulation
    # -------------------------------------------------------------
    print("\n[Phase 2] SIMULATING DISASTER: Destroying primary host!")
    print(f"  Purging local files: {PRIMARY_DB}...")
    cleanup_db(PRIMARY_DB)
    print("  ✓ Primary local database destroyed. Zero local state remains.")

    # -------------------------------------------------------------
    # Phase 3: Cold-Start Hydration from S3
    # -------------------------------------------------------------
    print("\n[Phase 3] Starting Cold-Start Hydration from MinIO S3...")
    print(f"  Target path: {RESTORED_DB}")

    try:
        Arkilian.hydrate_s3(
            db_path=RESTORED_DB,
            endpoint=S3_CONFIG["endpoint"],
            bucket=S3_CONFIG["bucket"],
            region=S3_CONFIG["region"],
            access_key=S3_CONFIG["access_key"],
            secret_key=S3_CONFIG["secret_key"],
            prefix=S3_CONFIG["prefix"]
        )
        print("  ✓ Hydration completed successfully!")
    except Exception as e:
        print(f"  ✗ Hydration failed: {e}")
        sys.exit(1)

    # -------------------------------------------------------------
    # Phase 4: Data Parity & Integrity Verification
    # -------------------------------------------------------------
    print("\n[Phase 4] Opening Hydrated Database to verify integrity...")
    os.environ["ARKILIAN_ENABLE_BACKUP"] = "0"
    with Arkilian(RESTORED_DB) as recovered_db:
        recovered_count = recovered_db.all("SELECT COUNT(*) as cnt FROM audit_orders")[0]["cnt"]
        print(f"  Recovered DB record count: {recovered_count}")

        if recovered_count != primary_count:
            print(f"  ✗ Data mismatch! Expected {primary_count}, got {recovered_count}")
            sys.exit(1)

        print("\n  Sample records from recovered database:")
        samples = recovered_db.all("SELECT order_id, customer_id, total_amount, status FROM audit_orders LIMIT 5")
        for row in samples:
            print(f"    - Order #{row['order_id']}: Customer {row['customer_id']} | ${row['total_amount']:.2f} | Status: {row['status']}")

    cleanup_db(RESTORED_DB)
    print("\n=== Disaster Recovery & Verification Finished Successfully! ===")


if __name__ == "__main__":
    main()
