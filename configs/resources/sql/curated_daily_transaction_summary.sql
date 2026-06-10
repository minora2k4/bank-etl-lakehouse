-- Query phục vụ dashboard từ bảng summary đã tổng hợp.
SELECT
    transaction_date,
    channel,
    province,
    SUM(total_transactions) AS total_transactions,
    SUM(total_amount_vnd) AS total_amount_vnd,
    AVG(failed_rate) AS avg_failed_rate
FROM read_csv_auto('lakehouse/curated/daily_transaction_summary.csv')
GROUP BY transaction_date, channel, province
ORDER BY transaction_date, channel, province;
