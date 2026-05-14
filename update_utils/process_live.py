import warnings
warnings.filterwarnings('ignore')

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from poly_utils.utils import get_markets

CHUNK_SIZE = 2_000_000  # rows per chunk


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

    if not os.path.isdir('data/processed'):
        os.makedirs('data/processed')

    # Load markets
    markets_df = get_markets()
    markets_df = markets_df.rename({'id': 'market_id'})

    markets_long = (
        markets_df
        .select(["market_id", "token1", "token2"])
        .melt(id_vars="market_id", value_vars=["token1", "token2"],
              variable_name="side", value_name="asset_id")
    )

    schema_overrides = {
        "takerAssetId": pl.Utf8,
        "makerAssetId": pl.Utf8,
        "makerAmountFilled": pl.Utf8,
        "takerAmountFilled": pl.Utf8,
    }

    # Resume point
    start_row = 0
    if os.path.isfile(progress_file) and os.path.isfile(processed_file):
        with open(progress_file, 'r') as f:
            start_row = int(f.read().strip())
        print(f"📍 Resuming from row {start_row:,}")
    else:
        print("⚠ Starting from beginning")
        if os.path.isfile(processed_file):
            os.remove(processed_file)

    row_index = 0
    chunk_num = 0
    first_write = not os.path.isfile(processed_file)

    reader = pl.read_csv_batched(
        input_file,
        schema_overrides=schema_overrides,
        batch_size=CHUNK_SIZE,
        truncate_ragged_lines=True,
    )

    while True:
        batches = reader.next_batches(1)
        if not batches:
            break

        chunk = batches[0]
        chunk_start = row_index
        chunk_end = row_index + len(chunk)
        row_index = chunk_end

        # Skip already processed
        if chunk_end <= start_row:
            chunk_num += 1
            continue

        # Trim partial chunk on resume
        if chunk_start < start_row:
            chunk = chunk.slice(start_row - chunk_start)

        chunk_num += 1
        print(f"  Chunk {chunk_num}: rows {chunk_start:,}-{chunk_end:,} ({len(chunk):,} rows)...")

        # Cast amounts
        chunk = chunk.with_columns([
            pl.col("makerAmountFilled").cast(pl.Float64, strict=False),
            pl.col("takerAmountFilled").cast(pl.Float64, strict=False),
            pl.from_epoch(pl.col('timestamp'), time_unit='s').alias('timestamp'),
        ])

        # Identify non-USDC asset
        chunk = chunk.with_columns(
            pl.when(pl.col("makerAssetId") != "0")
            .then(pl.col("makerAssetId"))
            .otherwise(pl.col("takerAssetId"))
            .alias("nonusdc_asset_id")
        )

        # Join market info
        chunk = chunk.join(markets_long, left_on="nonusdc_asset_id", right_on="asset_id", how="left")

        # Label asset sides
        chunk = chunk.with_columns([
            pl.when(pl.col("makerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("makerAsset"),
            pl.when(pl.col("takerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("takerAsset"),
            pl.col("market_id"),
        ])

        # Compute trade fields
        chunk = chunk.with_columns([
            pl.when(pl.col("makerAsset") != "USDC")
            .then(pl.col("makerAsset"))
            .otherwise(pl.col("takerAsset"))
            .alias("nonusdc_side"),

            pl.when(pl.col("takerAsset") == "USDC")
            .then(pl.lit("BUY"))
            .otherwise(pl.lit("SELL"))
            .alias("taker_direction"),

            pl.when(pl.col("takerAsset") == "USDC")
            .then(pl.lit("SELL"))
            .otherwise(pl.lit("BUY"))
            .alias("maker_direction"),

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

            # last_price skipped — set to null, resolved markets dominate scoring anyway
            pl.lit(None).cast(pl.Float64).alias("last_price"),
        ])

        output = chunk.select([
            'timestamp', 'market_id', 'maker', 'taker',
            'nonusdc_side', 'maker_direction', 'taker_direction',
            'price', 'usd_amount', 'token_amount', 'transactionHash'
        ])

        if first_write:
            output.write_csv(processed_file)
            first_write = False
        else:
            with open(processed_file, mode="a") as f:
                output.write_csv(f, include_header=False)

        with open(progress_file, 'w') as f:
            f.write(str(chunk_end))

        print(f"  ✓ Wrote {len(output):,} rows | Progress saved at row {chunk_end:,}")

    if os.path.isfile(progress_file):
        os.remove(progress_file)

    print("\n" + "=" * 60)
    print("✅ Processing complete!")
    print("=" * 60)


if __name__ == "__main__":
    process_live()
