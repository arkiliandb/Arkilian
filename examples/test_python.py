#!/usr/bin/env python3
"""Arkilian — Python integration test"""

import sys
import os

# Add bindings dir to path so we can import arkilian
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bindings', 'python'))

from arkilian import Arkilian

DB_FILE = 'test_python.sqlite'

def main():
    # Open database
    db = Arkilian(DB_FILE)
    print('✓ Database opened')

    # Create table
    db.exec('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
    print('✓ Table created')

    # Insert with parameters
    r1 = db.run('INSERT INTO users (name, age) VALUES (?, ?)', 'Alice', 30)
    print(f'✓ Inserted Alice (rowid: {r1.last_insert_rowid})')

    r2 = db.run('INSERT INTO users (name, age) VALUES (?, ?)', 'Bob', 25)
    print(f'✓ Inserted Bob (rowid: {r2.last_insert_rowid})')

    r3 = db.run('INSERT INTO users (name, age) VALUES (?, ?)', 'Charlie', 35)
    print(f'✓ Inserted Charlie (rowid: {r3.last_insert_rowid})')

    # Query all rows
    all_users = db.all('SELECT * FROM users')
    print(f'✓ All users: {all_users}')

    # Query with parameters
    older = db.all('SELECT name, age FROM users WHERE age > ?', 28)
    print(f'✓ Users older than 28: {older}')

    # Update
    updated = db.run('UPDATE users SET age = ? WHERE name = ?', 31, 'Alice')
    print(f'✓ Updated {updated.changes} row(s)')

    # Delete
    deleted = db.run('DELETE FROM users WHERE name = ?', 'Charlie')
    print(f'✓ Deleted {deleted.changes} row(s)')

    # Final state
    final = db.all('SELECT * FROM users ORDER BY id')
    print(f'✓ Final state: {final}')

    # Context manager test
    with Arkilian(DB_FILE) as db2:
        count = db2.all('SELECT COUNT(*) as cnt FROM users')
        print(f'✓ Context manager works, count: {count[0]["cnt"]}')

    # Close
    db.close()
    print('✓ Database closed')

    # Cleanup
    os.unlink(DB_FILE)
    print('\n✅ All Python tests passed!')

if __name__ == '__main__':
    main()
