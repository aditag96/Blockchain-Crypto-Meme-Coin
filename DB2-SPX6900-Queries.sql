1. SPX6900 PRICE COMPARISON WITH PEERS 
    ## SPX - 180 Days Avg Price
        SELECT DATE(minute) AS day, symbol, blockchain, avg(price) as avg_price
        FROM prices.usd -- table to get coin prices
        WHERE symbol = 'SPX' -- Filter Coin
        AND blockchain = 'ethereum' -- Filter Blockchain
        AND price IS NOT NULL AND price != 0
        AND minute >= date_add('day', -180, current_date) -- Data from last 180 days 
        GROUP BY DATE(minute), symbol, blockchain ORDER BY day DESC;

    ## Pepe Coin - 180 Days Avg Price
        SELECT DATE(minute) AS day, symbol, blockchain, avg(price) AS avg_price
        FROM prices.usd -- table to get coin prices
        WHERE symbol = 'PEPECOIN' -- Filter Coin
        AND blockchain = 'ethereum' -- Filter Blockchain
        AND price IS NOT NULL AND price != 0
        AND minute >= date_add('day', -180, current_date) -- Data from last 180 days 
        GROUP BY DATE(minute), symbol, blockchain ORDER BY day DESC;

    ## Turbo - 180 Days Avg Price
        SELECT DATE(minute) AS day, symbol, blockchain, avg(price) AS avg_price
        FROM prices.usd -- table to get coin prices
        WHERE symbol = 'TURBO' -- Filter Coin
        AND blockchain = 'ethereum' -- Filter Blockchain
        AND price IS NOT NULL AND price != 0
        AND minute >= date_add('day', -180, current_date) -- Data from last 180 days 
        GROUP BY DATE(minute), symbol, blockchain ORDER BY day DESC;

    ## MEME - 180 Days Avg Price
        SELECT DATE(minute) AS day, symbol, blockchain, avg(price) AS avg_price
        FROM prices.usd -- table to get coin prices
        WHERE symbol = 'MEME' -- Filter Coin
        AND blockchain = 'ethereum' -- Filter Blockchain
        AND price IS NOT NULL AND price != 0
        AND minute >= date_add('day', -180, current_date) -- Data from last 180 days 
        GROUP BY DATE(minute), symbol, blockchain ORDER BY day DESC;

2. SPX6900 Transaction Volumes and Unique Users Performing Trsnsactions
        WITH daily_transactions AS ( -- CTE to extract daily transactions and volumes
        SELECT
        DATE_TRUNC('day', block_time) AS transaction_day, tx_from AS sender_wallet, tx_to AS receiver_wallet, token_bought_amount AS volume_traded
        FROM dex.trades -- Table to get transactions and wallet/ user details
        WHERE token_bought_address = 0xe0f63a424a4439cbe457d80e4f4b51ad25b2c56c -- Filter Coin
        ),
        aggregated_transactions AS (
        SELECT -- Using Sub Query
        transaction_day, COUNT(*) AS transaction_count, SUM(volume_traded) AS total_volume, COUNT(DISTINCT sender_wallet) AS unique_senders,
        COUNT(DISTINCT receiver_wallet) AS unique_receivers FROM daily_transactions GROUP BY transaction_day
        )
        SELECT transaction_day, transaction_count, total_volume, unique_senders, unique_receivers FROM aggregated_transactions
        WHERE total_volume > 1000000  -- Filter for days with total volume greater than 1 Million as Row Level Filter
        ORDER BY transaction_day;

3. SPX6900 Number of Transfer Transactions and Unique Buyers and Sellers for 2024 
        SELECT Buyers, Sellers, Transactions FROM (
        SELECT -- Using Sub Query
        COUNT(DISTINCT "to") AS Buyers, COUNT(DISTINCT "from") AS Sellers, COUNT(1) AS Transactions
        FROM erc20_ethereum.evt_Transfer -- table to get transfer transactions and distinct users
        WHERE evt_block_time > timestamp '2024-01-01' -- 2024 Date filter
        AND contract_address = 0xE0f63A424a4439cBE457D80E4f4b51aD25b2c56C -- Filter Coin
        ) AS TransferSummary;

4. SPX6900 Daily and Total Volumes for last 6 months 
        WITH TradeData AS ( -- using CTE to get trade volumes
        SELECT DATE_TRUNC('day', block_time) AS Day, token_bought_symbol AS Symbol, amount_usd
        FROM dex.trades -- table to get volumes
        WHERE block_time >= CAST('2024-05-20' AS TIMESTAMP) -- last 6 months volumes
        AND token_bought_address = 0xe0f63a424a4439cbe457d80e4f4b51ad25b2c56c -- Filter Token
        AND blockchain = 'ethereum' -- Filter Blockchain
        ) SELECT Day, Symbol, 
       ROUND(SUM(amount_usd), 2) AS Daily_Volume, -- Rounded to 2 decimal places
       ROUND(SUM(SUM(amount_usd)) OVER (PARTITION BY symbol ORDER BY day), 2) AS Total_Vol -- Rounded to 2 decimal places
        FROM TradeData GROUP BY day, symbol ORDER BY day DESC;

5. SPX6900 Number of Buy/ Sell Transactions for last 6 Months 
        WITH token_transactions AS ( -- CTE to extract token transaction types
        SELECT DATE_TRUNC('day', block_time) AS transaction_day,
        CASE
           WHEN token_bought_address = FROM_HEX('e0f63a424a4439cbe457d80e4f4b51ad25b2c56c') THEN 'buy' -- Filter Coin
           WHEN token_sold_address = FROM_HEX('e0f63a424a4439cbe457d80e4f4b51ad25b2c56c') THEN 'sell' -- Filter Coin
        END AS transaction_type FROM dex.trades -- Table to get transaction details
        WHERE (token_bought_address = FROM_HEX('e0f63a424a4439cbe457d80e4f4b51ad25b2c56c')
         OR token_sold_address = FROM_HEX('e0f63a424a4439cbe457d80e4f4b51ad25b2c56c'))
        AND block_time >= CURRENT_DATE - INTERVAL '180' DAY -- last 180 days
        )
        SELECT transaction_day,COUNT(CASE WHEN transaction_type = 'buy' THEN 1 END) AS buy_transactions,
       COUNT(CASE WHEN transaction_type = 'sell' THEN 1 END) AS sell_transactions FROM token_transactions 
       GROUP BY transaction_day ORDER BY transaction_day;

6. SPX6900 Best Trading Hour based on last 90 Days 
        WITH token_transactions AS ( -- CTE to get hours of day
        SELECT DATE_TRUNC('hour', block_time) AS transaction_hour, block_time
        FROM dex.trades -- table to get transactions
        WHERE (token_bought_address = FROM_HEX('e0f63a424a4439cbe457d80e4f4b51ad25b2c56c')
         OR token_sold_address = FROM_HEX('e0f63a424a4439cbe457d80e4f4b51ad25b2c56c')) -- Filter Coin
        )       
        SELECT EXTRACT(HOUR FROM transaction_hour) AS trading_hour, -- Total transactions in the last 90 days
        COUNT(*) AS total_transactions_90_days
        FROM token_transactions WHERE block_time >= CURRENT_DATE - INTERVAL '90' DAY 
        GROUP BY EXTRACT(HOUR FROM transaction_hour) ORDER BY trading_hour;

7. SPX6900 52 Week Price Stats 
        SELECT blockchain, symbol,
        ROUND(AVG(price), 5) AS "52_week_avg_price", -- Average price for the last 360 days (52-week average price)
        ROUND(MIN(price), 5) AS "52_week_low", -- Minimum price for the last 360 days (52-week low)
        ROUND(MAX(price), 5) AS "52_week_high", -- Maximum price for the last 360 days (52-week high)
        DATE(MINUTE) AS "52_week_low_day", -- Day with the lowest price for the last 360 days (52-week low day)
        DATE(MAX(MINUTE)) AS "52_week_high_day", -- Day with the highest price for the last 360 days (52-week high day)
        ROUND(MAX(price) - AVG(price), 5) AS "price_diff_360_days", -- Price difference between max and average price over the last 360 days
        ROUND((MAX(price) - GREATEST(MIN(price), 0.01)) / GREATEST(MIN(price), 0.01) * 100,
        5) AS "% 52_week_price_fluctuation" -- Percentage difference in price over 360 days (52-week price fluctuation)
        FROMprices.usd p
        WHERE
        blockchain = 'ethereum' -- Filter Blockchain
        AND symbol = 'SPX' -- Filter Coin
        AND minute >= CURRENT_TIMESTAMP - INTERVAL '360' DAY -- last 360 days
        GROUP BY blockchain, symbol ORDER BY symbol;

8. SPX6900 Pricing Fluctuations on Daily Basis for last 90 Days 
        WITH daily_prices AS ( -- CTE to get day and price
        SELECT blockchain, symbol, DATE_TRUNC('day', minute) AS day, AVG(price) AS daily_avg_price
        FROM prices.usd p -- table to get prices
        WHERE blockchain = 'ethereum' -- Filter blockchain
        AND symbol IN ('SPX') -- Filter Coin
        AND minute >= CURRENT_TIMESTAMP - INTERVAL '90' DAY -- 90 days Interval
        GROUP BY blockchain, symbol, DATE_TRUNC('day', minute))
        SELECT blockchain AS Blockchain, symbol AS Symbol, day AS Day,
        ROUND(daily_avg_price, 2) AS Daily_Avg_Price, -- Rounding the price to 2 decimal places
        ROUND((daily_avg_price - LAG(daily_avg_price) OVER (PARTITION BY symbol ORDER BY day)) / NULLIF(LAG(daily_avg_price) OVER (PARTITION BY symbol ORDER BY day), 0) * 100,
        2) AS Price_Percentage_Change -- Price percentage change
        FROM daily_prices
        WHERE day >= DATE '2024-06-01' -- Filter for dates after June 1st, 2024
        AND day <= DATE '2024-11-20' -- Filter for dates before November 20th, 2024
        ORDER BY symbol, day DESC;