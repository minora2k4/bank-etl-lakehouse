DROP TABLE IF EXISTS ledger_entries;
DROP TABLE IF EXISTS processed_transactions;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS repayments;
DROP TABLE IF EXISTS loans;
DROP TABLE IF EXISTS cards;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS merchants;
DROP TABLE IF EXISTS branches;

CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    full_name TEXT,
    gender TEXT,
    dob DATE,
    age INTEGER,
    province TEXT,
    district TEXT,
    occupation TEXT,
    monthly_income_vnd NUMERIC,
    customer_segment TEXT,
    phone_hash TEXT,
    email_hash TEXT,
    cccd_hash TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE branches (
    branch_id TEXT PRIMARY KEY,
    branch_name TEXT,
    province TEXT,
    district TEXT,
    branch_type TEXT
);

CREATE TABLE accounts (
    account_id TEXT PRIMARY KEY,
    customer_id TEXT,
    account_type TEXT,
    open_date DATE,
    balance_vnd NUMERIC,
    status TEXT,
    branch_id TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE cards (
    card_id TEXT PRIMARY KEY,
    customer_id TEXT,
    account_id TEXT,
    card_type TEXT,
    card_number_masked TEXT,
    issued_date DATE,
    expiry_date DATE,
    status TEXT
);

CREATE TABLE merchants (
    merchant_id TEXT PRIMARY KEY,
    merchant_name TEXT,
    merchant_category TEXT,
    province TEXT,
    risk_level TEXT
);

CREATE TABLE loans (
    loan_id TEXT PRIMARY KEY,
    customer_id TEXT,
    loan_type TEXT,
    loan_amount_vnd NUMERIC,
    interest_rate_pct NUMERIC,
    term_months INTEGER,
    start_date DATE,
    end_date DATE,
    loan_status TEXT
);

CREATE TABLE repayments (
    repayment_id TEXT PRIMARY KEY,
    loan_id TEXT,
    customer_id TEXT,
    due_date DATE,
    paid_date DATE,
    due_amount_vnd NUMERIC,
    paid_amount_vnd NUMERIC,
    days_past_due INTEGER,
    repayment_status TEXT
);

CREATE TABLE transactions (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT,
    customer_id TEXT,
    amount_vnd NUMERIC,
    transaction_type TEXT,
    transaction_time TIMESTAMP,
    status TEXT,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE processed_transactions (
    transaction_id TEXT PRIMARY KEY,
    kafka_topic TEXT,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    status TEXT NOT NULL,
    error_message TEXT,
    processed_at TIMESTAMP DEFAULT now(),
    posted_at TIMESTAMP
);

CREATE TABLE ledger_entries (
    ledger_id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    debit_vnd NUMERIC DEFAULT 0,
    credit_vnd NUMERIC DEFAULT 0,
    balance_after_vnd NUMERIC NOT NULL,
    entry_type TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);
