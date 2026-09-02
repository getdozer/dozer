-- Create tables
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    income DECIMAL(10,2),
    risk_tolerance VARCHAR(20)
);

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    amount DECIMAL(10,2),
    category VARCHAR(50),
    merchant VARCHAR(100),
    transaction_date TIMESTAMP
);

CREATE TABLE credit_cards (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    annual_fee DECIMAL(10,2),
    rewards_rate DECIMAL(5,2),
    category VARCHAR(50),
    description TEXT
);

-- Insert sample data
INSERT INTO customers (name, age, income, risk_tolerance) VALUES
('Alice', 30, 75000, 'moderate'),
('Bob', 45, 120000, 'aggressive'),
('Charlie', 25, 50000, 'conservative');

INSERT INTO transactions (customer_id, amount, category, merchant, transaction_date) VALUES
(1, 1200, 'travel', 'Delta Airlines', '2024-01-15 10:00:00'),
(1, 85, 'dining', 'Olive Garden', '2024-01-20 19:30:00'),
(1, 200, 'groceries', 'Whole Foods', '2024-02-01 12:00:00'),
(2, 500, 'travel', 'Marriott', '2024-02-10 15:00:00'),
(2, 150, 'dining', 'Ruth''s Chris', '2024-02-12 20:00:00'),
(3, 60, 'groceries', 'Kroger', '2024-02-14 11:00:00');

INSERT INTO credit_cards (product_name, annual_fee, rewards_rate, category, description) VALUES
('Travel Rewards Card', 95, 2.0, 'travel', 'Earn 2x points on travel and dining'),
('Cash Back Card', 0, 1.5, 'groceries', 'Unlimited 1.5% cash back on all purchases'),
('Premium Rewards Card', 550, 3.0, 'travel', 'Luxury travel benefits and 3x points on travel'),
('Student Card', 0, 1.0, 'groceries', 'No annual fee, 1% cash back on all purchases');
