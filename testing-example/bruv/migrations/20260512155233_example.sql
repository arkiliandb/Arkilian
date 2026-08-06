-- --> up
-- Up migration
CREATE TABLE users (
    id text PRIMARY KEY NOT NULL,
     email TEXT UNIQUE NOT NULL,
     name TEXT
);

-- --> down
DROP TABLE users;
-- Down migration
