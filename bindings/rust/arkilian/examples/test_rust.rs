use arkilian::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_file = "test_rust.db";

    // Open database
    let mut db = Database::new(db_file)?;
    db.set_token("dummy-test-token-00000000-0000-0000-0000-000000000000")?;
    println!("✓ Database opened with token");

    // Create table
    db.exec("DROP TABLE IF EXISTS users")?;
    db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")?;
    println!("✓ Table created");

    // Insert with parameters using String
    let alice = String::from("Alice");
    db.run("INSERT INTO users (name, age) VALUES (?, ?)", &[&alice as &dyn arkilian::ToSql, &30])?;
    println!("✓ Inserted Alice");

    let bob = String::from("Bob");
    db.run("INSERT INTO users (name, age) VALUES (?, ?)", &[&bob as &dyn arkilian::ToSql, &25])?;
    println!("✓ Inserted Bob");

    let charlie = String::from("Charlie");
    db.run("INSERT INTO users (name, age) VALUES (?, ?)", &[&charlie as &dyn arkilian::ToSql, &35])?;
    println!("✓ Inserted Charlie");

    // Query all rows
    let all_users = db.all("SELECT * FROM users", &[])?;
    println!("✓ All users:");
    for row in &all_users {
        for (name, value) in row {
            println!("    {}={}", name, value);
        }
    }

    // Query with parameters
    let age_filter = 28_i32;
    let older = db.all("SELECT name, age FROM users WHERE age > ?", &[&age_filter as &dyn arkilian::ToSql])?;
    println!("✓ Users older than 28:");
    for row in &older {
        for (name, value) in row {
            println!("    {}={}", name, value);
        }
    }

    // Update
    let new_age = 31_i32;
    db.run("UPDATE users SET age = ? WHERE name = ?", &[&new_age as &dyn arkilian::ToSql, &alice])?;
    println!("✓ Updated Alice's age");

    // Delete
    db.run("DELETE FROM users WHERE name = ?", &[&charlie as &dyn arkilian::ToSql])?;
    println!("✓ Deleted Charlie");

    // Final state
    let final_rows = db.all("SELECT * FROM users ORDER by id", &[])?;
    println!("✓ Final state:");
    for row in &final_rows {
        for (name, value) in row {
            println!("    {}={}", name, value);
        }
    }

    // db auto-closes on drop
    drop(db);
    println!("✓ Database closed");

    // Cleanup
    std::fs::remove_file(db_file)?;
    println!("\n✅ All Rust tests passed!");

    Ok(())
}