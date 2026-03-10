"""
oracle_fetcher.py
-----------------
Fetches historical asset prices directly from onchain Chainlink-style oracle contracts.

WHY THIS EXISTS:
  - Dune Analytics doesn't ingest prices for all assets (especially new/niche tokens)
  - CoinGecko/CoinMarketCap free tiers cap at 30 days; paid tiers are expensive
  - Aggregator prices are averages of CEX/DEX feeds — not the exact prices a protocol uses
  - Querying the oracle contract the protocol itself uses gives you the ground-truth price

HOW IT WORKS:
  1. For each target timestamp, binary-search the blockchain for the closest block number
  2. Call latestRoundData() on the oracle contract at that block
  3. Normalize the result using the oracle's decimals() and record it
  4. Repeat across all tokens in parallel, saving results to CSV

REQUIREMENTS:
  pip install web3 pandas requests urllib3

USAGE:
  1. Set RPC_URL to any public or private Ethereum (or EVM-compatible) RPC endpoint
  2. Add your token oracle addresses to `TOKEN_ORACLES`
  3. Adjust date range and interval as needed
  4. Run: python oracle_fetcher.py
"""

from web3 import Web3
from web3.exceptions import BadFunctionCallOutput, ContractLogicError
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import os

# Any public or private EVM RPC endpoint
# Public options: https://ethereum-rpc.publicnode.com, https://rpc.ankr.com/eth
# Private (higher rate limits): Alchemy, Infura, QuickNode
RPC_URL = "https://ethereum-rpc.publicnode.com"

# Fetch window: from (END_DATE - DAYS_BACK) to END_DATE
END_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
DAYS_BACK = 204  # Number of days of history to fetch

# How frequently to sample prices (in hours)
# 1 = hourly, 24 = daily, 168 = weekly
INTERVAL_HOURS = 1

# Output file — new rows are appended; existing data is preserved on resume
CSV_OUTPUT = "oracle_prices.csv"

# Number of parallel threads (reduce if hitting RPC rate limits)
MAX_WORKERS = 8

# Derived global start date (used as default for tokens without a specific start date)
GLOBAL_START_DATE = END_DATE - timedelta(days=DAYS_BACK)


# TOKEN ORACLE ADDRESSES
# Maps token symbol → oracle contract address (Chainlink-compatible interface)

# How to find oracle addresses:
#   - Aave V3: call getSourceOfAsset(tokenAddress) on the AaveOracle contract
#   - Other protocols: check their docs or Etherscan for price feed / oracle contracts

# Example Aave V3 Oracle (Ethereum): 0x54586bE62E3c3580375aE3723C145253060Ca0C2

TOKEN_ORACLES = {
    "hbHYPE":    "0xDb924A25BfF353f98B066F692c38C3cFacb3a601",
    "hbUSDT":    "0x96572d32d699cE463Fdf36610273CC76B7d83f9b",
    "wVLP":      "0xA9fFe62E785324cb39cB5E2B3Ef713674391d31F",
    "hbXAUT":    "0xf3dB9f59f9C90495D1c9556fC5737A679720921d",
    "lstHYPE":   "0x2b959a9Deb8e62FaaEA1b226F3bbcbcC0Af31560",
    "hbBTC":     "0x9ED559c2Ad1562aE8e919691A84A3320f547B248",
    "liquidHYPE":"0x1CeaB703956e24b18a0AF6b272E0bF3F499aCa0F",
    "hbUSDC":    "0xc82CAd78983436BddfcAf0F21316207D87b87462",
}

# Per-token start dates (optional).
# Useful for newly listed tokens whose oracle didn't exist from GLOBAL_START_DATE.
# Tokens not listed here default to GLOBAL_START_DATE.
TOKEN_START_DATES = {
    "hbHYPE":    datetime(2025, 10, 15),
    "hbUSDT":    datetime(2025, 10, 15),
    "wVLP":      datetime(2025, 10, 15),
    "hbXAUT":    datetime(2025, 10, 15),
    "lstHYPE":   datetime(2025, 10, 15),
    "hbBTC":     datetime(2025, 10, 15),
    "liquidHYPE":datetime(2025, 10, 15),
    "hbUSDC":    datetime(2025, 10, 15),
}


# ORACLE ABI (Chainlink-compatible interface)

# Most DeFi price oracles expose this standard interface.
# decimals()        → precision of the price (e.g. 8 means divide answer by 1e8)
# latestRoundData() → (roundId, answer, startedAt, updatedAt, answeredInRound)
#                      `answer` is the raw price (divide by 10**decimals to get USD price)

ORACLE_ABI = [
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"internalType": "uint80",  "name": "roundId",         "type": "uint80"},
            {"internalType": "int256",  "name": "answer",          "type": "int256"},
            {"internalType": "uint256", "name": "startedAt",       "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt",       "type": "uint256"},
            {"internalType": "uint80",  "name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function"
    }
]


# THREAD-LOCAL WEB3 INSTANCES
# Each thread gets its own Web3 connection to avoid sharing state across threads.
# Using a session with retry logic handles transient RPC errors gracefully.

_web3_local = threading.local()


def get_web3() -> Web3:
    """
    Returns a thread-local Web3 instance with connection pooling and retry logic.
    Creates a new instance the first time it's called in a given thread.
    """
    if not hasattr(_web3_local, "w3"):
        session = requests.Session()

        # Retry on common transient errors: rate limits (429) and server errors (5xx)
        retry = Retry(
            total=3,
            backoff_factor=0.2,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry, pool_maxsize=20)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        _web3_local.w3 = Web3(Web3.HTTPProvider(
            RPC_URL,
            session=session,
            request_kwargs={"timeout": 15}
        ))

    return _web3_local.w3



# BLOCK CACHE

# Binary-searching for blocks is expensive (multiple RPC calls per lookup).
# We cache timestamp → block_number results, keyed by hour, so repeated lookups
# within the same run (e.g. across multiple tokens) hit the cache instead.

_block_cache: dict = {}
_cache_lock = threading.Lock()
_csv_lock = threading.Lock()


def find_block_at_timestamp(target_ts: datetime) -> int:
    """
    Binary-searches the chain to find the block number closest to `target_ts`.

    The search window is [block 1, latest block]. We stop when the found block's
    timestamp is within 30 minutes of the target, or after 20 iterations.

    Results are cached by hour to avoid redundant RPC calls.

    Args:
        target_ts: The datetime to find a block for.

    Returns:
        Block number closest to the target timestamp.
    """
    # Round to the hour for cache key consistency
    cache_key = target_ts.replace(minute=0, second=0, microsecond=0)

    with _cache_lock:
        if cache_key in _block_cache:
            return _block_cache[cache_key]

    w3 = get_web3()
    left = 1
    right = w3.eth.block_number
    best_block = right
    iterations = 0

    while left <= right and iterations < 20:
        mid = left + (right - left) // 2

        try:
            block = w3.eth.get_block(mid)
            block_dt = datetime.fromtimestamp(block["timestamp"])

            # Close enough (within 30 minutes) — accept this block
            if abs((block_dt - target_ts).total_seconds()) < 1800:
                best_block = mid
                break

            if block_dt > target_ts:
                right = mid - 1
            else:
                left = mid + 1

        except Exception:
            # On RPC error, move left and try a lower block
            right = mid - 1

        iterations += 1

    with _cache_lock:
        _block_cache[cache_key] = best_block

    return best_block


# ORACLE QUERY

def query_oracle_at_block(oracle, block: int):
    """
    Calls latestRoundData() on the oracle contract at the given block number.

    This returns the price that was active at that block — exactly what the
    protocol would have read if it queried its oracle at that moment.

    Args:
        oracle: A web3 contract instance (with ORACLE_ABI).
        block:  The block number to query at.

    Returns:
        (price: float, updated_at: datetime) on success, or (None, None) on error.
    """
    try:
        decimals = oracle.functions.decimals().call(block_identifier=block)
        round_data = oracle.functions.latestRoundData().call(block_identifier=block)

        _, answer, _, updated_at, _ = round_data

        # Sanity check: answer=0 or updated_at=0 indicates a stale/invalid round
        if answer == 0 or updated_at == 0:
            return None, None

        price = int(answer) / (10 ** decimals)
        price_ts = datetime.fromtimestamp(updated_at)

        return price, price_ts

    except (BadFunctionCallOutput, ContractLogicError, ValueError):
        # Expected errors: oracle not deployed at this block, call reverted, etc.
        return None, None
    except Exception as e:
        print(f"    Unexpected error at block {block}: {str(e)[:80]}")
        return None, None

# PER-TOKEN FETCH LOGIC

def fetch_token_prices(token: str, oracle_address: str) -> list[dict]:
    """
    Fetches hourly (or per INTERVAL_HOURS) price samples for a single token.

    Handles resume logic: if the CSV already has data for this token, fetching
    starts from the last recorded timestamp rather than the beginning.

    Args:
        token:          Token symbol (e.g. "hbBTC")
        oracle_address: Checksum address of the token's oracle contract

    Returns:
        List of row dicts ready to be written to CSV.
    """
    start_time = time.time()
    w3 = get_web3()

    # Determine start date
    # Use per-token start date if set, otherwise fall back to GLOBAL_START_DATE
    token_start = TOKEN_START_DATES.get(token, GLOBAL_START_DATE)
    loop_start = token_start.replace(hour=0, minute=0, second=0, microsecond=0)

    # Resume from existing data 
    # If the output CSV already has rows for this token, start from where we left off
    if os.path.exists(CSV_OUTPUT):
        with _csv_lock:
            try:
                df_existing = pd.read_csv(CSV_OUTPUT)
                token_rows = df_existing[df_existing["symbol"] == token.lower()]

                if not token_rows.empty:
                    last_ts = pd.to_datetime(token_rows["sample_timestamp"].max(), format="mixed")
                    loop_start = (last_ts + timedelta(hours=INTERVAL_HOURS)).replace(
                        minute=0, second=0, microsecond=0
                    )
                    print(f"  {token}: Resuming from {loop_start.strftime('%Y-%m-%d %H:00')} "
                          f"({len(token_rows)} existing samples)")
            except Exception as e:
                print(f"  {token}: Could not read existing CSV ({e}), starting fresh")

    # Clamp to the token's intended start date (don't go earlier)
    effective_start = max(loop_start, token_start.replace(hour=0, minute=0, second=0))

    if effective_start > END_DATE:
        print(f"  {token}: Already up to date, nothing to fetch")
        return []

    # Estimate total work
    total_hours = int((END_DATE - effective_start).total_seconds() / 3600)
    total_samples = (total_hours // INTERVAL_HOURS) + 1

    oracle = w3.eth.contract(address=oracle_address, abi=ORACLE_ABI)
    rows = []
    current_ts = effective_start
    samples_done = 0
    last_log_time = time.time()

    print(f"  {token}: Fetching {total_samples} samples "
          f"({effective_start.strftime('%Y-%m-%d')} → {END_DATE.strftime('%Y-%m-%d')})")

    while current_ts <= END_DATE:
        sample_ts = current_ts.replace(minute=0, second=0, microsecond=0)

        if sample_ts > END_DATE:
            break

        # Find the block closest to this timestamp, then query the oracle at that block
        block = find_block_at_timestamp(sample_ts)
        price, _ = query_oracle_at_block(oracle, block)

        if price is not None:
            rows.append({
                "date":             sample_ts.strftime("%Y-%m-%d"),
                "sample_timestamp": sample_ts.strftime("%Y-%m-%d %H:%M"),
                "symbol":           token.lower(),
                "price":            round(price, 8),
                "interval_hours":   INTERVAL_HOURS,
            })

        samples_done += 1
        current_ts += timedelta(hours=INTERVAL_HOURS)

        # Print progress every 5 seconds so long runs stay observable
        if time.time() - last_log_time >= 5:
            elapsed = time.time() - start_time
            rate = samples_done / elapsed if elapsed > 0 else 0
            eta = (total_samples - samples_done) / rate if rate > 0 else 0
            pct = (samples_done / total_samples) * 100

            print(f"    {token}: {samples_done}/{total_samples} ({pct:.1f}%) | "
                  f"{rate:.1f} samples/s | ETA {eta / 60:.1f} min | "
                  f"at {sample_ts.strftime('%Y-%m-%d %H:00')}")
            last_log_time = time.time()

    elapsed = time.time() - start_time
    rate = len(rows) / elapsed if elapsed > 0 else 0
    print(f"  {token}: ✓ {len(rows)} samples fetched in {elapsed:.1f}s ({rate:.1f}/s)")

    return rows

# CSV SAVE (THREAD-SAFE)

def save_rows_to_csv(rows: list[dict]) -> None:
    """
    Appends new rows to the output CSV.

    Handles deduplication (by symbol + timestamp) so that rerunning or resuming
    never creates duplicate entries. Thread-safe via _csv_lock.

    Args:
        rows: List of row dicts from fetch_token_prices()
    """
    if not rows:
        return

    with _csv_lock:
        new_df = pd.DataFrame(rows)

        if os.path.exists(CSV_OUTPUT):
            existing_df = pd.read_csv(CSV_OUTPUT)
            combined = pd.concat([existing_df, new_df]).drop_duplicates(
                subset=["symbol", "sample_timestamp"],
                keep="last"
            )
        else:
            combined = new_df

        combined = combined.sort_values(["sample_timestamp", "symbol"])
        combined.to_csv(CSV_OUTPUT, index=False)

# MAIN — PARALLEL EXECUTION

if __name__ == "__main__":
    print("=" * 70)
    print(f"Onchain Oracle Price Fetcher")
    print(f"  Tokens:   {len(TOKEN_ORACLES)}")
    print(f"  Period:   {GLOBAL_START_DATE.date()} → {END_DATE.date()}")
    print(f"  Interval: every {INTERVAL_HOURS}h")
    print(f"  Threads:  {MAX_WORKERS}")
    print(f"  Output:   {CSV_OUTPUT}")
    print("=" * 70)

    overall_start = time.time()
    pending_rows = []  # Buffer rows; flush to disk every 1000

    # Run one thread per token. Threads share the block cache but have their
    # own Web3 connections (thread-local), so there's no contention on RPC state.
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_token_prices, token, address): token
            for token, address in TOKEN_ORACLES.items()
        }

        for future in as_completed(futures):
            token = futures[future]
            try:
                rows = future.result()
                pending_rows.extend(rows)

                # Flush to disk when buffer is large enough to be worth it
                if len(pending_rows) >= 1000:
                    save_rows_to_csv(pending_rows)
                    pending_rows = []

            except Exception as e:
                print(f"  {token}: ✗ Unhandled error — {e}")

    # remove any remaining rows that didn't hit the 1000-row threshold
    if pending_rows:
        save_rows_to_csv(pending_rows)

    # Summary 
    total_time = time.time() - overall_start
    print("\n" + "=" * 70)
    print(f"✓ Done in {total_time:.1f}s ({total_time / 60:.1f} minutes)")

    if os.path.exists(CSV_OUTPUT):
        df = pd.read_csv(CSV_OUTPUT)
        print(f"✓ Total rows:  {len(df):,}")
        print(f"✓ Date range:  {df['date'].min()} → {df['date'].max()}")
        print(f"✓ Tokens:      {', '.join(sorted(df['symbol'].unique()))}")
        print(f"✓ Avg per token: {total_time / len(TOKEN_ORACLES):.1f}s")
    print("=" * 70)
