import os
import unittest
import arkilian
from arkilian import Arkilian, ARK_HF_ALL_CORE, HYDRATION_OK

TEST_DB = "test_py_parity.db"

class TestArkilianPythonParity(unittest.TestCase):
    def setUp(self):
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
        self.db = Arkilian(TEST_DB)

    def tearDown(self):
        if hasattr(self, "db") and self.db:
            self.db.close()
        for f in [TEST_DB, f"{TEST_DB}-wal", f"{TEST_DB}-shm", f"{TEST_DB}-journal", f"{TEST_DB}.arklock"]:
            if os.path.exists(f):
                os.remove(f)

    def test_crud_and_changes(self):
        self.db.exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, count INT, price REAL, data BLOB)")
        self.db.run("INSERT INTO items (name, count, price, data) VALUES (?, ?, ?, ?)", ["item1", 42, 19.99, b"bin\x00data"])
        self.assertEqual(self.db.changes, 1)
        self.assertEqual(self.db.last_insert_rowid, 1)

        rows = self.db.all("SELECT id, name, count, price, data FROM items WHERE id = ?", [1])
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["id"], 1)
        self.assertEqual(row["name"], "item1")
        self.assertEqual(row["count"], 42)
        self.assertAlmostEqual(row["price"], 19.99)
        self.assertEqual(row["data"], b"bin\x00data")

    def test_batch_transactions(self):
        self.db.exec("CREATE TABLE tx_test (id INTEGER PRIMARY KEY, val TEXT)")
        self.db.begin()
        self.db.exec("INSERT INTO tx_test (val) VALUES ('rollback_me')")
        self.db.rollback()
        rows = self.db.all("SELECT COUNT(*) as c FROM tx_test")
        self.assertEqual(rows[0]["c"], 0)

        self.db.begin()
        self.db.exec("INSERT INTO tx_test (val) VALUES ('commit_me')")
        self.db.commit()
        rows = self.db.all("SELECT COUNT(*) as c FROM tx_test")
        self.assertEqual(rows[0]["c"], 1)

    def test_monitoring_and_health_flags(self):
        self.db.exec("CREATE TABLE health_test (id INTEGER PRIMARY KEY, note TEXT)")
        self.assertIsInstance(self.db.backup_queue_depth, int)
        self.assertIsInstance(self.db.backup_oldest_pending_age_sec, int)
        self.assertIsInstance(self.db.backup_dead_letter_count, int)
        self.assertIsInstance(self.db.backup_thread_heartbeat_age_ms, int)
        self.assertIsInstance(self.db.backup_snapshot_heartbeat_age_ms, int)
        self.assertIsInstance(self.db.backup_trigger_coverage, int)
        self.assertIsInstance(self.db.backup_skipped_table_count, int)
        self.assertIsInstance(self.db.backup_chunk_count, int)
        self.assertIsInstance(self.db.backup_last_chunk_flush_age_ms, int)
        self.assertIsInstance(self.db.backup_health_flags, int)
        self.assertIsInstance(self.db.backup_healthy, bool)
        self.assertIsInstance(self.db.triggers_dirty, bool)
        self.assertIsInstance(self.db.capture_paused, bool)

    def test_backup_controls_and_wal(self):
        self.db.set_backup_enabled(False)
        self.assertFalse(self.db.backup_enabled)
        self.db.set_backup_enabled(True)
        self.assertTrue(self.db.backup_enabled)

        self.db.set_auto_resync_triggers(True)
        self.assertTrue(self.db.auto_resync_triggers)
        self.db.resync_triggers()

        pending = self.db.wal_pending
        self.assertIsInstance(pending, int)
        self.db.wal_flush()

    def test_constants_and_hydration_api(self):
        self.assertEqual(HYDRATION_OK, 0)
        self.assertEqual(ARK_HF_ALL_CORE, 0x1FF)
        # Verify hydrate_s3 is present
        self.assertTrue(callable(Arkilian.hydrate_s3))

if __name__ == "__main__":
    unittest.main()
