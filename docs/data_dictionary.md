# Data Dictionary

## Source tables

- `customers`: hồ sơ khách hàng, segment, thu nhập, hash PII.
- `accounts`: tài khoản CASA/SAVING/CREDIT theo customer.
- `cards`: thẻ ATM/DEBIT/CREDIT, chỉ lưu card masked.
- `transactions`: giao dịch VND theo kênh VPBANK_NEO, NAPAS_QR, ATM, POS, BRANCH.
- `loans`: khoản vay.
- `repayments`: lịch sử trả nợ.
- `merchants`: merchant/category/risk.
- `branches`: chi nhánh/phòng giao dịch.

## Curated files

- `dim_customer.csv`
- `dim_account.csv`
- `dim_card.csv`
- `dim_merchant.csv`
- `dim_branch.csv`
- `fact_transaction_YYYY-MM-DD.csv`
- `fact_loan.csv`
- `fact_repayment.csv`
- `daily_transaction_summary.csv`
- `customer_summary_YYYY-MM-DD.csv`
