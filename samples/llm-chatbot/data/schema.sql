CREATE TABLE customers (
    id TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    email TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    preferred_language TEXT NOT NULL,
    risk_tier TEXT NOT NULL
);

CREATE TABLE credit_cards (
    id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL REFERENCES customers(id),
    product_id TEXT NOT NULL,
    last_four TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE transactions (
    id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL REFERENCES customers(id),
    card_id TEXT NOT NULL REFERENCES credit_cards(id),
    merchant TEXT NOT NULL,
    category TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    transaction_date DATE NOT NULL
);

CREATE INDEX idx_transactions_customer_date ON transactions(customer_id, transaction_date);
CREATE INDEX idx_credit_cards_customer ON credit_cards(customer_id);
