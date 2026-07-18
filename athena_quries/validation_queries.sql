SHOW TABLES;

SELECT *
FROM api_stocks
LIMIT 10;

SELECT COUNT(*) AS total_rows
FROM api_stocks;

SELECT
  symbol,
  MIN(trade_date) AS earliest_trade_date,
  MAX(trade_date) AS latest_trade_date,
  ROUND(AVG(close_price), 2) AS avg_close_price,
  MAX(high_price) AS highest_price,
  MIN(low_price) AS lowest_price,
  SUM(volume) AS total_volume
FROM api_stocks
GROUP BY symbol;