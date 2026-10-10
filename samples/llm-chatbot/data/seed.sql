INSERT INTO customers VALUES
    ('customer-001', 'Priya Nair', 'priya.nair@example.com', 'Seattle', 'WA', 'English', 'low'),
    ('customer-002', 'Daniel Okafor', 'daniel.okafor@example.com', 'Austin', 'TX', 'English', 'medium');

INSERT INTO credit_cards VALUES
    ('card-001', 'customer-001', 'travel-card', '4102', 'active'),
    ('card-002', 'customer-001', 'cashback-card', '7789', 'active'),
    ('card-003', 'customer-002', 'secured-card', '3311', 'active');

INSERT INTO transactions VALUES
    ('transaction-001', 'customer-001', 'card-001', 'SeaTac Airport Parking', 'travel', 4200, DATE '2026-09-02'),
    ('transaction-002', 'customer-001', 'card-001', 'Northwest Coffee', 'dining', 1850, DATE '2026-09-05'),
    ('transaction-003', 'customer-001', 'card-002', 'Cascadia Grocers', 'groceries', 9430, DATE '2026-09-08'),
    ('transaction-004', 'customer-002', 'card-003', 'Apex Fuel Station', 'fuel', 6120, DATE '2026-09-03');
