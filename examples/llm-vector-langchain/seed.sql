-- =============================================================================
-- Dozer + LLM + Vector Database + LangChain Sample (getdozer/dozer#1690)
-- PostgreSQL Logical Replication & CDC Seed Script
-- =============================================================================

CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(32) PRIMARY KEY,
    full_name VARCHAR(128) NOT NULL,
    email VARCHAR(128) NOT NULL,
    credit_score INTEGER NOT NULL,
    annual_income DOUBLE PRECISION NOT NULL,
    account_tier VARCHAR(64) NOT NULL,
    preferred_reward_type VARCHAR(128) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    txn_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32) NOT NULL REFERENCES customers(customer_id),
    merchant_name VARCHAR(128) NOT NULL,
    category VARCHAR(64) NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    txn_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS credit_card_products (
    card_id VARCHAR(64) PRIMARY KEY,
    card_name VARCHAR(128) NOT NULL,
    category_focus VARCHAR(64) NOT NULL,
    reward_rate_pct DOUBLE PRECISION NOT NULL,
    annual_fee DOUBLE PRECISION NOT NULL,
    apr_pct DOUBLE PRECISION NOT NULL,
    min_credit_score INTEGER NOT NULL,
    min_annual_income DOUBLE PRECISION NOT NULL,
    welcome_bonus VARCHAR(128) NOT NULL,
    perks_summary TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS documents (
    doc_id VARCHAR(32) PRIMARY KEY,
    title VARCHAR(256) NOT NULL,
    category VARCHAR(64) NOT NULL,
    card_id VARCHAR(64) NOT NULL,
    min_credit_score INTEGER NOT NULL,
    content TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Enable full before/after row payloads for PostgreSQL CDC logical replication
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;
ALTER TABLE credit_card_products REPLICA IDENTITY FULL;
ALTER TABLE documents REPLICA IDENTITY FULL;

-- -----------------------------------------------------------------------------
-- 1. Customers (4 distinct banking personas)
-- -----------------------------------------------------------------------------
INSERT INTO customers (customer_id, full_name, email, credit_score, annual_income, account_tier, preferred_reward_type, updated_at) VALUES
('CUST-001', 'Ava Sharma', 'ava.sharma@example.com', 785, 145000.0, 'Platinum', 'Travel & Airport Lounge', '2026-01-15 09:00:00'),
('CUST-002', 'Marcus Vance', 'marcus.vance@example.com', 715, 82000.0, 'Gold', 'Cashback & Groceries', '2026-01-15 09:05:00'),
('CUST-003', 'Elena Rostova', 'elena.rostova@example.com', 645, 48000.0, 'Standard', 'Credit Building & Low APR', '2026-01-15 09:10:00'),
('CUST-004', 'David Chen', 'david.chen@example.com', 810, 210000.0, 'Private Client', 'Business & Technology', '2026-01-15 09:15:00');

-- -----------------------------------------------------------------------------
-- 2. Credit Card Products (5 tiers with eligibility thresholds)
-- -----------------------------------------------------------------------------
INSERT INTO credit_card_products (card_id, card_name, category_focus, reward_rate_pct, annual_fee, apr_pct, min_credit_score, min_annual_income, welcome_bonus, perks_summary) VALUES
('CARD-TRAVEL-SIG', 'Dozer Sapphire Reserve Travel', 'Travel', 5.0, 295.0, 19.9, 750, 100000.0, '75,000 bonus points ($1,125 travel value)', '5% on flights & hotels, 3% on global dining, unlimited Priority Pass airport lounge access, $300 annual travel credit, primary rental car collision coverage, zero foreign transaction fees.'),
('CARD-CASH-PLUS', 'Dozer Everyday Cashback Plus', 'Groceries', 4.0, 0.0, 18.4, 690, 50000.0, '$250 cash statement credit', '4% cashback on supermarkets and grocery delivery, 3% cashback on dining and streaming, 1.5% on all other purchases, zero annual fee.'),
('CARD-BUILDER-SEC', 'Dozer Horizon Credit Builder', 'Credit Building', 1.5, 0.0, 14.9, 600, 25000.0, '$100 automatic credit line graduation bonus', '1.5% flat cashback on every purchase, $0 annual fee, low 14.9% variable APR, free monthly FICO credit score monitoring, automatic credit limit review after 6 on-time payments.'),
('CARD-TECH-EXEC', 'Dozer Quantum Executive Commercial', 'Technology', 5.0, 195.0, 17.5, 760, 130000.0, '100,000 commercial points ($1,250 cloud/SaaS credit)', '5% back on cloud computing, SaaS subscriptions, and electronics hardware, 3% on business travel, complimentary global Wi-Fi and executive concierge.'),
('CARD-DINING-GOLD', 'Dozer Culinary Gold Rewards', 'Dining', 4.5, 95.0, 19.2, 700, 65000.0, '50,000 culinary bonus points ($600 dining value)', '4.5% rewards at restaurants, cafes, and food delivery worldwide, 2% at grocery stores, $120 annual dining statement credit.');

-- -----------------------------------------------------------------------------
-- 3. Transactions (14 transactions across Travel, Dining, Groceries, Technology)
-- -----------------------------------------------------------------------------
INSERT INTO transactions (txn_id, customer_id, merchant_name, category, amount, txn_timestamp) VALUES
('TXN-1001', 'CUST-001', 'Singapore Airlines Direct', 'Travel', 2450.0, '2026-02-01 10:15:00'),
('TXN-1002', 'CUST-001', 'Grand Hyatt Tokyo', 'Travel', 1800.0, '2026-02-03 14:20:00'),
('TXN-1003', 'CUST-001', 'Nobu Downtown', 'Dining', 680.0, '2026-02-05 20:45:00'),
('TXN-1004', 'CUST-001', 'Osteria Mozza', 'Dining', 500.0, '2026-02-09 19:30:00'),
('TXN-1005', 'CUST-001', 'Whole Foods Market', 'Groceries', 320.0, '2026-02-11 11:10:00'),
('TXN-1006', 'CUST-002', 'Whole Foods Market', 'Groceries', 940.0, '2026-02-02 17:05:00'),
('TXN-1007', 'CUST-002', 'Trader Joes', 'Groceries', 700.0, '2026-02-08 16:40:00'),
('TXN-1008', 'CUST-002', 'Sweetgreen & Local Bistro', 'Dining', 620.0, '2026-02-12 13:00:00'),
('TXN-1009', 'CUST-002', 'Delta Air Lines', 'Travel', 240.0, '2026-02-14 08:30:00'),
('TXN-1010', 'CUST-003', 'Neighborhood Grocery Co-Op', 'Groceries', 410.0, '2026-02-04 18:15:00'),
('TXN-1011', 'CUST-003', 'Corner Cafe', 'Dining', 190.0, '2026-02-10 12:25:00'),
('TXN-1012', 'CUST-004', 'AWS Cloud Infrastructure', 'Technology', 3600.0, '2026-02-01 09:00:00'),
('TXN-1013', 'CUST-004', 'Apple Enterprise Store', 'Technology', 2200.0, '2026-02-06 15:50:00'),
('TXN-1014', 'CUST-004', 'Lufthansa Business Class', 'Travel', 3100.0, '2026-02-13 22:10:00');

-- -----------------------------------------------------------------------------
-- 4. Policy & Product Knowledge Documents (8 documents for Vector Store RAG)
-- -----------------------------------------------------------------------------
INSERT INTO documents (doc_id, title, category, card_id, min_credit_score, content, updated_at) VALUES
('DOC-001', 'Dozer Sapphire Reserve Travel — Airport Lounge & Travel Benefits Guide', 'Travel', 'CARD-TRAVEL-SIG', 750, 'The Dozer Sapphire Reserve Travel card (CARD-TRAVEL-SIG) earns 5% rewards on flights, airlines, hotels, and travel bookings, plus 3% on worldwide dining. Cardholders receive unlimited complimentary Priority Pass Select airport lounge access for the primary cardholder and two guests, a $300 annual travel statement credit, $100 Global Entry or TSA PreCheck credit, trip cancellation insurance up to $10,000, and zero foreign transaction fees. Requires minimum credit score 750 and annual income $100,000. Annual fee is $295.', '2026-02-01 00:00:00'),
('DOC-002', 'Dozer Everyday Cashback Plus — Grocery & Family Rewards Schedule', 'Groceries', 'CARD-CASH-PLUS', 690, 'The Dozer Everyday Cashback Plus card (CARD-CASH-PLUS) is designed for household grocery and supermarket spending, earning 4% cashback on groceries and supermarket delivery up to $25,000 annually, 3% cashback on dining and streaming services, and 1.5% on all other purchases. It features a $0 annual fee and a $250 cash statement credit welcome bonus. Requires minimum credit score 690 and annual income $50,000.', '2026-02-01 00:00:00'),
('DOC-003', 'Dozer Horizon Credit Builder — Low APR & Credit Score Graduation Policy', 'Credit Building', 'CARD-BUILDER-SEC', 600, 'The Dozer Horizon Credit Builder card (CARD-BUILDER-SEC) helps customers with credit scores starting at 600 and annual income from $25,000 build or rebuild credit history while earning 1.5% flat cashback on every purchase. It carries a $0 annual fee, a low 14.9% APR, free monthly FICO score alerts, and an automatic credit limit graduation review after 6 consecutive on-time monthly payments plus a $100 graduation bonus.', '2026-02-01 00:00:00'),
('DOC-004', 'Dozer Quantum Executive Commercial — Cloud, SaaS & Technology Rebate Terms', 'Technology', 'CARD-TECH-EXEC', 760, 'The Dozer Quantum Executive Commercial card (CARD-TECH-EXEC) provides 5% rewards on technology spend including cloud hosting (AWS, GCP, Azure), SaaS software subscriptions, and enterprise computer hardware, plus 3% on business travel flights and hotels. Includes a 100,000-point welcome bonus ($1,250 cloud/SaaS value) and global Boingo Wi-Fi. Requires minimum credit score 760 and annual income $130,000. Annual fee is $195.', '2026-02-01 00:00:00'),
('DOC-005', 'Dozer Culinary Gold Rewards — Restaurant & Entertainment Perks', 'Dining', 'CARD-DINING-GOLD', 700, 'The Dozer Culinary Gold Rewards card (CARD-DINING-GOLD) earns 4.5% rewards on restaurants, fine dining, cafes, and food delivery services worldwide, plus 2% at grocery stores. Cardholders receive a $120 annual dining credit ($10/month) and 50,000 culinary bonus points. Requires minimum credit score 700 and annual income $65,000. Annual fee is $95.', '2026-02-01 00:00:00'),
('DOC-006', 'Platinum & Private Client Annual Fee Waiver & Relationship Pricing Policy', 'Policy', 'ALL', 740, 'Customers in the Platinum or Private Client account tiers who maintain an annual income above $140,000 or total quarterly card spend exceeding $5,000 qualify for priority concierge support and a 25% relationship points multiplier on redemption for international travel and technology statement credits.', '2026-02-01 00:00:00'),
('DOC-007', 'Travel Protection, Flight Delay Reimbursement & Baggage Insurance Disclosure', 'Travel', 'CARD-TRAVEL-SIG', 750, 'When airfare is purchased with Dozer Sapphire Reserve Travel (CARD-TRAVEL-SIG), cardholders are covered by automatic flight delay reimbursement ($500 per ticket for delays over 4 hours), lost luggage reimbursement up to $3,000 per passenger, and primary rental car collision damage waiver worldwide.', '2026-02-01 00:00:00'),
('DOC-008', 'Responsible Credit Utilization & Tier Upgrade Eligibility Guidelines', 'Credit Building', 'CARD-BUILDER-SEC', 600, 'Customers holding Dozer Horizon Credit Builder (CARD-BUILDER-SEC) who raise their credit score above 690 and reach $50,000 annual income become automatically eligible in Dozer real-time underwriting to upgrade to Dozer Everyday Cashback Plus (CARD-CASH-PLUS) or Dozer Culinary Gold Rewards (CARD-DINING-GOLD) without losing accrued cashback rewards.', '2026-02-01 00:00:00');
