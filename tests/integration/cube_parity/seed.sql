-- Seed data for Cube.js parity tests
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    user_id INTEGER NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    city VARCHAR(100) NOT NULL
);

INSERT INTO users (id, name, email, city) VALUES
(1, 'Alice',   'alice@example.com',   'New York'),
(2, 'Bob',     'bob@example.com',     'London'),
(3, 'Charlie', 'charlie@example.com', 'Tokyo'),
(4, 'Diana',   'diana@example.com',   'New York');

INSERT INTO orders (id, status, user_id, amount, created_at) VALUES
(1,  'completed',  1, 100.00, '2024-01-15 10:00:00'),
(2,  'completed',  2, 250.00, '2024-01-15 11:00:00'),
(3,  'pending',    1,  50.00, '2024-02-01 09:00:00'),
(4,  'completed',  3, 175.00, '2024-02-15 14:00:00'),
(5,  'cancelled',  2,  30.00, '2024-03-01 08:00:00'),
(6,  'completed',  4, 300.00, '2024-03-15 16:00:00'),
(7,  'pending',    3,  80.00, '2024-04-01 12:00:00'),
(8,  'completed',  1, 120.00, '2024-04-15 10:30:00'),
(9,  'completed',  2, 200.00, '2024-05-01 09:30:00'),
(10, 'pending',    4,  90.00, '2024-05-15 15:00:00'),
(11, 'completed',  3, 350.00, '2024-06-01 11:00:00'),
(12, 'cancelled',  1,  40.00, '2024-06-15 13:00:00');
