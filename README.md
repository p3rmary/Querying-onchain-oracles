# Querying-onchain-oracles

Query token/asset oracle contracts directly from the blockchain to get accurate historical price feeds — no subscriptions, no CEX/DEX averages, just the exact prices your protocol uses.

---

## The Problem

When analyzing DeFi protocols, you often need **accurate historical asset prices**. The common options fall short:

| Source | Problem |
|--------|---------|
| **Dune Analytics** | Doesn't ingest prices for all assets, especially newly listed or niche tokens |
| **CoinGecko / CoinMarketCap** | Free tier limited to 30 days of history; paid subscription required for more; prices are averages of CEX/DEX feeds — not what the protocol actually uses |
| **Protocol dashboards** | Usually don't expose raw historical price data |

**The real solution:** Query the oracle contract the protocol itself uses. This gives you the *exact* price the protocol used at any point in time — not an approximation.

---

## How It Works

Most DeFi protocols (Aave, Compound, etc.) rely on **Chainlink-style oracle contracts** to get asset prices onchain. These contracts expose a standard interface:

- `latestRoundData()` — returns the latest price and timestamp
- `decimals()` — returns the precision of the price feed

By querying these at specific block numbers (derived from timestamps via binary search), you can reconstruct the full historical price series at any interval you want.

```
Target timestamp → Binary search → Block number → Oracle.latestRoundData() → Price
```

---

## Features

-  **Exact protocol prices** — same source the protocol uses, not CEX/DEX averages
-  **Any historical depth** — no 30-day limits, no paywalls
-  **Configurable intervals** — hourly, daily, or custom
-  **Parallel fetching** — multi-threaded across tokens for speed
-  **Resume support** — picks up from where it left off if interrupted
-  **Block caching** — avoids redundant RPC calls for timestamp→block lookups
-  **CSV output** — ready for analysis in Dune, pandas, Excel, etc.
-  **Works with any public RPC** — no API key required

---

## Quickstart

### 1. Install dependencies

```bash
pip install web3 pandas requests urllib3
```

### 2. Find your oracle addresses

For Aave, oracle addresses can be found in the protocol's docs or by calling `getSourceOfAsset()` on the [AaveOracle contract](https://docs.aave.com/developers/core-contracts/aaveoracle).

For other protocols, check their docs or look up the oracle address on Etherscan.

### 3. Configure the script

Edit the top of `oracle_fetcher.py`:

```python
# RPC endpoint (any public or private Ethereum node)
RPC_URL = 'https://ethereum-rpc.publicnode.com'

# Date range
END_DATE = datetime.now()
DAYS_BACK = 204          # How far back to fetch
INTERVAL_HOURS = 1       # Sampling frequency (1 = hourly)

# Your tokens and their oracle contract addresses
token_oracles = {
    "WBTC": "0x...",   # Oracle address for WBTC
    "USDC": "0x...",   # Oracle address for USDC
    # Add more tokens here
}

# Optional: set a specific start date per token (useful for newly listed assets)
token_start_dates = {
    "WBTC": datetime(2024, 1, 1),
}
```

### 4. Run

```bash
python oracle_fetcher.py
```

Output is saved to `aave_prices.csv` (configurable via `CSV_OUTPUT`).

---

## Output Format

```
date,sample_timestamp,symbol,price,interval_hours
2025-01-01,2025-01-01 00:00,wbtc,42350.12345678,1
2025-01-01,2025-01-01 01:00,wbtc,42401.87654321,1
...
```

---

## Finding Oracle Addresses

### Aave V3 (Ethereum)
Call `getSourceOfAsset(tokenAddress)` on the AaveOracle contract:
- **AaveOracle**: `0x54586bE62E3c3580375aE3723C145253060Ca0C2`

### General approach
1. Go to the protocol's docs → deployed contracts
2. Find the oracle or price feed contract
3. Look for `getSourceOfAsset`, `getPriceFeed`, or similar methods
4. The returned address is your oracle — plug it in

---

## How Block Search Works

The script uses **binary search** to find the block closest to a target timestamp:

```python
def binary_search_block(target_ts):
    left, right = 1, latest_block
    while left <= right:
        mid = (left + right) // 2
        block_ts = get_block_timestamp(mid)
        if close_enough(block_ts, target_ts):
            return mid
        elif block_ts > target_ts:
            right = mid - 1
        else:
            left = mid + 1
```

Results are cached by hour, so repeated queries for the same time window are fast.

---

## Configuration Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `RPC_URL` | publicnode | Ethereum RPC endpoint |
| `END_DATE` | now | End of the fetch window |
| `DAYS_BACK` | 204 | Days of history to fetch |
| `INTERVAL_HOURS` | 1 | Sampling frequency in hours |
| `CSV_OUTPUT` | `aave_prices.csv` | Output file path |
| `MAX_WORKERS` | 8 | Parallel threads (lower if hitting RPC limits) |

---

## Tips

- **RPC rate limits**: If you hit rate limits, reduce `MAX_WORKERS` or use a private RPC (Alchemy, Infura, etc.)
- **Newly listed tokens**: Use `token_start_dates` to avoid querying blocks before a token's oracle existed
- **Resume after interruption**: The script detects existing CSV data and resumes from the last fetched timestamp automatically
- **Other chains**: Works on any EVM chain — just swap `RPC_URL` for an RPC on that chain (Arbitrum, Base, Optimism, etc.)

---

## Requirements

```
web3>=6.0.0
pandas>=1.5.0
requests>=2.28.0
urllib3>=1.26.0
```

---

## Contributing

PRs welcome! Some ideas:
- Support for other oracle standards (Pyth, Redstone, Chronicle)
- Automatic oracle address lookup from Aave/Compound contracts
- Direct export to Parquet or DuckDB

---

## License

MIT
