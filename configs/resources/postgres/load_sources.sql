\copy customers FROM '/workspace/data/source_db/customers.csv' WITH (FORMAT csv, HEADER true, NULL '');
\copy branches FROM '/workspace/data/source_db/branches.csv' WITH (FORMAT csv, HEADER true, NULL '');
\copy accounts FROM '/workspace/data/source_db/accounts.csv' WITH (FORMAT csv, HEADER true, NULL '');
\copy cards FROM '/workspace/data/source_db/cards.csv' WITH (FORMAT csv, HEADER true, NULL '');
\copy merchants FROM '/workspace/data/source_db/merchants.csv' WITH (FORMAT csv, HEADER true, NULL '');
\copy loans FROM '/workspace/data/source_db/loans.csv' WITH (FORMAT csv, HEADER true, NULL '');
\copy repayments FROM '/workspace/data/source_db/repayments.csv' WITH (FORMAT csv, HEADER true, NULL '');
