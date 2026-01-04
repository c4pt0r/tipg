-- UUID Type Tests

DROP TABLE IF EXISTS contacts;

CREATE TABLE contacts (
    contact_id uuid DEFAULT gen_random_uuid(),
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    email VARCHAR NOT NULL,
    phone VARCHAR,
    PRIMARY KEY (contact_id)
);

-- Insert with auto-generated UUID
INSERT INTO contacts (first_name, last_name, email, phone)
VALUES ('John', 'Smith', 'john.smith@example.com', '408-237-2345')
RETURNING *;

INSERT INTO contacts (first_name, last_name, email, phone)
VALUES ('Jane', 'Doe', 'jane.doe@example.com', '408-237-2346')
RETURNING *;

-- Verify UUIDs are unique
SELECT * FROM contacts;

-- Insert with explicit UUID
INSERT INTO contacts (contact_id, first_name, last_name, email, phone)
VALUES ('550e8400-e29b-41d4-a716-446655440000'::uuid, 'Bob', 'Wilson', 'bob@example.com', '408-111-2222')
RETURNING *;

-- Query by UUID
SELECT * FROM contacts WHERE contact_id = '550e8400-e29b-41d4-a716-446655440000'::uuid;

-- Test gen_random_uuid() directly
SELECT gen_random_uuid();

DROP TABLE contacts;
