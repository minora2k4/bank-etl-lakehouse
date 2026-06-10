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
    created_at TIMESTAMP
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
