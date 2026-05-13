import warnings
warnings.filterwarnings('ignore')

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from poly_utils.utils import get_markets, update_missing_tokens

import subprocess
import pandas as pd

CHUNK_SIZE = 5_000_000  # rows per chunk — adjust down if Railway still OOMs

def get_processed_df(df: pl.DataFrame, last_price_df: pl.DataFrame, markets_long: pl.DataFrame) -> pl.DataFrame:
    """
    Process a chunk of raw trades into the final schema.
    last_price_df: pre-computed last_price per (market_id, nonusdc_side)
    markets_long: pre-computed market side lookup
    """

    # 2) Identify the non-USDC asset for each trade
    df = df.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )

    # 3) Join to recover market + side
    df = df.join(
        markets_long,
        left_on="nonusdc_asset_id",
        right_on="asset_id",
        how="left",
    )

    # 4) Label columns
    df = df.with_columns([
        pl.when(pl.col("makerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("makerAsset"),
        pl.when(pl.col("takerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("takerAsset"),
        pl.col("market_id"),
    ])

    df = df[['timestamp', 'market_id', 'maker', 'makerAsset', 'makerAmountFilled', 'taker', 'takerAsset', 'takerAmountFilled', 'transactionHash']]

    df = df.with_columns([
        (pl.col("makerAmountFilled") / 10**6).alias("makerAmountFilled"),
        (pl.col("takerAmountFilled") / 10**6).alias("takerAmountFilled"),
    ])

    df = df.with_columns([
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("BUY"))
        .otherwise(pl.lit("SELL"))
        .alias("taker_direction"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("SELL"))
        .otherwise(pl.lit("BUY"))
        .alias("maker_direction"),
    ])

    df = df.with_columns([
        pl.when(pl.col("makerAsset") != "USDC")
        .then(pl.col("makerAsset"))
        .otherwise(pl.col("takerAsset"))
        .alias("nonusdc_side"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled"))
        .alias("usd_amount"),

        pl.when(pl.col("takerAsset") != "USDC")
        .then(pl.col("takerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled"))
        .alias("token_amount"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled") / pl.col("makerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled") / pl.col("takerAmountFilled"))
        .cast(pl.Float64)
        .alias("price"),
    ])

    # Join pre-computed last_price
    df = df.join(
        last_price_df,
        on=["market_id", "nonusdc_side"],
        how="left",
    )

    df = df[['timestamp', 'market_id', 'maker', 'taker', 'nonusdc_side', 'maker_direction', 'taker_direction', 'price', 'usd_amount', 'token_amount', 'transactionHash']]
    return df


def process_live():
    processed_file = 'data/processed/trades.csv'
    input_file = 'data/goldsky/orderFilled.csv'
    progress_file = 'data/processed/progress.txt'

    print("=" * 60)
    print("🔄 Processing Live Trades (chunked mode)")
    print("=" * 60)

    if not os.path.isfile(input_file):
        print(f"❌ Input file not found: {input_file}")
        return

    # --- Load markets ---
    markets_df = get_markets()
    markets_df = markets_df.rename({'id': 'market_id'})

    markets_long = (
        markets_df
        .select(["market_id", "token1", "token2"])
        .melt(id_vars="market_id", value_vars=["token1", "token2"],
              variable_name="side", value_name="asset_id")
    )

    # --- Pass 1: Compute last_price per (market_id, nonusdc_side) lazily ---
    print("\n📊 Pass 1: Computing last_price per market side (lazy scan)...")

    schema_overrides = {
        "takerAssetId": pl.Utf8,
        "makerAssetId": pl.Utf8,
        "makerAmountFilled": pl.Utf8,
        "takerAmountFilled": pl.Utf8,
    }

    lazy = pl.scan_csv(input_file, schema_overrides=schema_overrides)

    lazy = lazy.with_columns([
        pl.col("makerAmountFilled").cast(pl.Float64, strict=False),
        pl.col("takerAmountFilled").cast(pl.Float64, strict=False),
        pl.from_epoch(pl.col('timestamp'), time_unit='s').alias('timestamp'),
    ])

    # Identify nonusdc_asset_id lazily
    lazy = lazy.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )

    # Join markets_long to get market_id and side
    lazy = lazy.join(
        markets_long.lazy(),
        left_on="nonusdc_asset_id",
        right_on="asset_id",
        how="left",
    )

    # Compute price lazily
    lazy = lazy.with_columns([
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled") / pl.col("makerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled") / pl.col("takerAmountFilled"))
        .cast(pl.Float64)
        .alias("price"),

        pl.when(pl.col("makerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("makerAsset"),
        pl.when(pl.col("takerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("takerAsset"),
    ])

    lazy = lazy.with_columns(
        pl.when(pl.col("makerAsset") != "USDC")
        .then(pl.col("makerAsset"))
        .otherwise(pl.col("takerAsset"))
        .alias("nonusdc_side")
    )

    # Get last price per (market_id, nonusdc_side)
    last_price_df = (
        lazy
        .group_by(["market_id", "nonusdc_side"])
        .agg(
            pl.col("price").sort_by("timestamp").last().alias("last_price")
        )
        .with_columns(
            pl.when(pl.col("last_price") > 0.98).then(pl.lit(1.0))
             .when(pl.col("last_price") < 0.02).then(pl.lit(0.0))
             .otherwise(pl.col("last_price"))
             .alias("last_price")
        )
        .collect()
    )

    print(f"✓ Computed last_price for {len(last_price_df):,} market sides")

    # --- Pass 2: Process in chunks ---
    print(f"\n⚙️  Pass 2: Processing in chunks of {CHUNK_SIZE:,} rows...")

    if not os.path.isdir('data/processed'):
        os.makedirs('data/processed')

    # Check resume point
    start_row = 0
    if os.path.isfile(progress_file) and os.path.isfile(processed_file):
        with open(progress_file, 'r') as f:
            start_row = int(f.read().strip())
        print(f"📍 Resuming from row {start_row:,}")
    else:
        print("⚠ Starting from beginning")
        # Clear any partial output
        if os.path.isfile(processed_file):
            os.remove(processed_file)

    row_index = 0
    chunk_num = 0
    first_write = not os.path.isfile(processed_file)

    reader = pl.read_csv_batched(
        input_file,
        schema_overrides=schema_overrides,
        batch_size=CHUNK_SIZE,
    )

    while True:
        batches = reader.next_batches(1)
        if not batches:
            break

        chunk = batches[0]
        chunk_start = row_index
        chunk_end = row_index + len(chunk)
        row_index = chunk_end

        # Skip already-processed chunks
        if chunk_end <= start_row:
            chunk_num += 1
            continue

        # Partial resume: trim rows already processed
        if chunk_start < start_row:
            chunk = chunk.slice(start_row - chunk_start)

        chunk_num += 1
        print(f"  Chunk {chunk_num}: rows {chunk_start:,}-{chunk_end:,} ({len(chunk):,} rows)...")

        # Cast amount columns
        chunk = chunk.with_columns([
            pl.col("makerAmountFilled").cast(pl.Float64, strict=False),
            pl.col("takerAmountFilled").cast(pl.Float64, strict=False),
        ])

        # Parse timestamp
        chunk = chunk.with_columns(
            pl.from_epoch(pl.col('timestamp'), time_unit='s').alias('timestamp')
        )

        # Process chunk
        processed_chunk = get_processed_df(chunk, last_price_df, markets_long)

        # Write output
        if first_write:
            processed_chunk.write_csv(processed_file)
            first_write = False
        else:
            with open(processed_file, mode="a") as f:
                processed_chunk.write_csv(f, include_header=False)

        # Save progress
        with open(progress_file, 'w') as f:
            f.write(str(chunk_end))

        print(f"  ✓ Wrote {len(processed_chunk):,} rows | Progress saved at row {chunk_end:,}")

    # Clean up progress file on success
    if os.path.isfile(progress_file):
        os.remove(progress_file)

    print("\n" + "=" * 60)
    print("✅ Processing complete!")
    print("=" * 60)


if __name__ == "__main__":
    process_live()
