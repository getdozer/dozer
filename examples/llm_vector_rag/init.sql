-- Sample schema for banking RAG demo
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    credit_score INT NOT NULL,
    monthly_income NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    category VARCHAR(50) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS card_products (
    id SERIAL PRIMARY KEY,
    card_name VARCHAR(100) NOT NULL,
    target_category VARCHAR(50) NOT NULL,
    reward_rate NUMERIC(4, 2) NOT NULL,
    annual_fee NUMERIC(6, 2) NOT NULL,
    min_credit_score INT NOT NULL
);

-- Seed initial data
INSERT INTO customers (name, email, credit_score, monthly_income) VALUES
('Alice Chen', 'alice@example.com', 760, 6500.00),
('Bob Miller', 'bob@example.com', 640, 3800.00),
('Carlos Gomez', 'carlos@example.com', 720, 5200.00);

INSERT INTO transactions (customer_id, category, amount) VALUES
(1, 'Travel', 1250.00),
(1, 'Travel', 820.00),
(1, 'Dining', 140.00),
(2, 'Groceries', 350.00),
(2, 'Utilities', 180.00),
(3, 'Dining', 450.00),
(3, 'Online Shopping', 320.00);

INSERT INTO card_products (card_name, target_category, reward_rate, annual_fee, min_credit_score) VALUES
('AeroMiles Voyager', 'Travel', 5.0, 95.00, 720),
('Gourmet Cashback', 'Dining', 4.0, 0.00, 680),
('Everyday Saver Plus', 'Groceries', 3.0, 0.00, 620),
('Executive Platinum Reserve', 'General', 2.0, 450.00, 750);
