-- Seed data for the Dozer + LLM chatbot sample.
-- Three operational datasets: customer profiles, transactions and a
-- credit-card product catalog.

CREATE TABLE customers (
    customer_id   BIGINT PRIMARY KEY,
    first_name    TEXT NOT NULL,
    last_name     TEXT NOT NULL,
    email         TEXT NOT NULL,
    city          TEXT NOT NULL,
    segment       TEXT NOT NULL,
    credit_score  INT  NOT NULL
);

CREATE TABLE transactions (
    txn_id      BIGINT PRIMARY KEY,
    customer_id BIGINT      NOT NULL,
    merchant    TEXT        NOT NULL,
    category    TEXT        NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL,
    txn_date    DATE        NOT NULL
);

CREATE TABLE products (
    product_id   BIGINT PRIMARY KEY,
    name         TEXT NOT NULL,
    category     TEXT NOT NULL,
    annual_fee   NUMERIC(8, 2) NOT NULL,
    apr          NUMERIC(5, 2) NOT NULL,
    credit_limit NUMERIC(8, 2) NOT NULL,
    rewards      TEXT NOT NULL,
    description  TEXT NOT NULL
);

INSERT INTO customers (customer_id, first_name, last_name, email, city, segment, credit_score) VALUES
  (1001, 'Alice',  'Nguyen',  'alice.nguyen@example.com',  'Singapore',     'premium',  782),
  (1002, 'Bruno',  'Costa',   'bruno.costa@example.com',   'Sao Paulo',     'mass',     655),
  (1003, 'Chloe',  'Dubois',  'chloe.dubois@example.com',  'Paris',         'premium',  811),
  (1004, 'Daniel', 'Kim',     'daniel.kim@example.com',    'Seoul',         'mass',     690),
  (1005, 'Elena',  'Rossi',   'elena.rossi@example.com',   'Milan',         'affluent', 745);

INSERT INTO transactions (txn_id, customer_id, merchant, category, amount, txn_date) VALUES
  (1, 1001, 'Wanderlust Airways',   'travel',      1250.00, '2026-09-28'),
  (2, 1001, 'Grand Plaza Hotel',    'travel',       640.50, '2026-09-29'),
  (3, 1001, 'Bistro Moderne',       'dining',        89.90, '2026-09-30'),
  (4, 1002, 'Supermercado Bom',     'groceries',    152.30, '2026-09-27'),
  (5, 1002, 'Cafe Central',         'dining',         18.75, '2026-09-28'),
  (6, 1002, 'Metro Transit',        'transport',     42.00, '2026-09-30'),
  (7, 1003, 'Mode Ensemble',        'shopping',     890.00, '2026-09-26'),
  (8, 1003, 'Artisan Gallery',      'shopping',    1450.00, '2026-09-29'),
  (9, 1003, 'Le Petit Bistrot',     'dining',       120.40, '2026-09-30'),
  (10, 1004, 'TechNova Store',      'electronics',  799.00, '2026-09-25'),
  (11, 1004, 'StreamFlix',          'entertainment',  15.99, '2026-09-28'),
  (12, 1004, 'Hankuk BBQ',          'dining',         67.20, '2026-09-29'),
  (13, 1005, 'Sempione Market',     'groceries',    210.15, '2026-09-26'),
  (14, 1005, 'Milano Centrale Taxi','transport',     35.50, '2026-09-27'),
  (15, 1005, 'Boutique Italia',     'shopping',     480.00, '2026-09-30'),
  (16, 1005, 'Trattoria da Vito',   'dining',        95.80, '2026-10-01'),
  (17, 1001, 'SkyLounge Bar',       'dining',       210.00, '2026-10-02'),
  (18, 1003, 'Boutique Hotel Lune', 'travel',       980.00, '2026-10-02');

INSERT INTO products (product_id, name, category, annual_fee, apr, credit_limit, rewards, description) VALUES
  (2001, 'Velocity Travel Rewards', 'travel', 195.00, 17.90, 15000.00,
   '3x points on flights and hotels',
   'Premium travel card with 3x points on flights and hotels, airport lounge access and no foreign transaction fees. Ideal for frequent flyers who book international travel.'),
  (2002, 'Everyday Cashback', 'cashback', 0.00, 15.90, 8000.00,
   '5% cashback on groceries and fuel',
   'No annual fee cashback card earning 5% back on groceries and fuel and 1% on everything else. A straightforward everyday card for households and commuters.'),
  (2003, 'Luxe Concierge', 'premium', 549.00, 16.90, 30000.00,
   '2x points everywhere plus concierge',
   'Invitation-style premium card with a personal concierge, fine-dining and hotel elite status, 2x points on all spending and complimentary premium subscriptions for high spenders.'),
  (2004, 'Student Starter', 'starter', 0.00, 19.90, 2000.00,
   '1% cashback, credit builder',
   'Entry-level card for students and first cardholders with 1% cashback, no annual fee and free credit-score monitoring to help build a credit history responsibly.'),
  (2005, 'Dining & Entertainment', 'dining', 95.00, 16.40, 10000.00,
   '4x points on restaurants and shows',
   'Rewards card focused on lifestyle spending with 4x points at restaurants, bars, theatres and streaming services, plus periodic dining-exclusive offers and tastings.');
