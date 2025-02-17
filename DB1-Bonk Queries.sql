1. Bonk Coin Details
    SELECT * FROM tokens_solana.fungible WHERE token_mint_address = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' -- filter coin

2. Bonk 180 Days Avg Pricing
    SELECT DATE(minute) AS day, symbol, blockchain, AVG(price) AS avg_price FROM prices.usd
    WHERE symbol = 'BONK'  -- filter token
    AND blockchain = 'solana'  -- filter blockchain
    AND price IS NOT NULL  -- exclude null prices
    AND price != 0  -- exclude zero prices
    AND minute >= CURRENT_DATE - INTERVAL 180 DAY  -- for last 180 days
    GROUP BY day, symbol, blockchain ORDER BY day DESC;

3. Bonk Daily Unique Wallets
    SELECT DATE(block_time) AS transaction_date, COUNT(DISTINCT trader_id) AS unique_wallets
    FROM dex_solana.trades -- Solana Blockchain data table
    WHERE token_bought_mint_address = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' -- Filter Coin
    AND block_time >= CURRENT_DATE - INTERVAL '30' DAY -- Last 30 days filter
    GROUP BY transaction_date ORDER BY transaction_date DESC, unique_wallets DESC;

4. Bonk - Weekly Average Transactions
    WITH weekly_avg AS (SELECT DATE_TRUNC('week', block_time) AS week_start_date, 
    ROUND(AVG(token_bought_amount_raw)) AS weekly_avg_transaction_size
    FROM dex_solana.trades 
    WHERE token_bought_mint_address = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' -- Filter Coin
    AND blockchain = 'solana' -- Filter Blockchain
    GROUP BY DATE_TRUNC('week', block_time))
    SELECT week_start_date, weekly_avg_transaction_size
    FROM weekly_avg
    WHERE weekly_avg_transaction_size > 1000000 -- Filter for transactions > 1 Million
    ORDER BY week_start_date DESC LIMIT 10;

5. Bonk - Top 10 Days by Transaction Volumes
    WITH filtered_transactions AS (
    SELECT block_time, account_keys FROM solana.transactions 
    WHERE block_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY -- Last 30 days filter
    AND success = true AND CONTAINS(account_keys, 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263') -- Filter Coin
    )
    SELECT DATE_TRUNC('day', block_time) AS Top_10_days, COUNT(*) AS transaction_volume
    FROM filtered_transactions GROUP BY DATE_TRUNC('day', block_time) ORDER BY transaction_volume DESC LIMIT 10;

6. Bonk Whales
    SELECT token_balance_owner AS Whales, MAX(token_balance) AS Token_Balance
    FROM solana_utils.latest_balances WHERE token_mint_address = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
    GROUP BY token_balance_owner HAVING MAX(token_balance) > 10000000 -- Row Filter for balances > 10 million
    ORDER BY Token_Balance DESC LIMIT 10;

7. Bonk Most Active Wallets
    SELECT trader_id as Active_Wallets, -- Wallet ID
    COUNT(*) AS transaction_count -- Total transactions by the wallet
    FROM dex_solana.trades --table to get wallet details by transactions
    WHERE token_bought_mint_address = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' -- Filter Coin
    AND block_time >= CURRENT_DATE - INTERVAL '30' DAY -- Last 30 days filter
    GROUP BY trader_id ORDER BY transaction_count DESC LIMIT 100;