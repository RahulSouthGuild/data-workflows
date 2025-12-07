"""
Azure Blob Storage Processing Utilities

Handles downloading, decompressing, and converting files from Azure Blob Storage
to Parquet format for efficient data processing.
"""

import asyncio
import gzip
import logging
from pathlib import Path
from typing import Optional
import polars as pl
import aiofiles
from azure.storage.blob.aio import ContainerClient

# Color codes for console output
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RESET = "\033[0m"

# Configuration
MAX_RETRIES = 3
RETRY_DELAY = 2
PARQUET_ROW_GROUP_SIZE = 100000


def clean_file_name(filename: str) -> str:
    """Clean filename to be safe for filesystem

    Args:
        filename: Original filename

    Returns:
        Cleaned filename
    """
    # Remove extension and normalize
    name = Path(filename).stem
    # Replace spaces and special chars
    name = name.replace(" ", "_").replace("-", "_")
    return name


async def download_blob(
    blob_path: str,
    container: ContainerClient,
    output_path: Path,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """Download blob from Azure Storage to local file

    Args:
        blob_path: Path of blob in container
        container: Azure BlobContainerClient
        output_path: Local path to save file
        logger: Optional logger instance

    Returns:
        True if successful, False otherwise
    """
    try:
        if logger:
            logger.info(f"📥 Downloading blob: {blob_path}")

        blob = container.get_blob_client(blob_path)
        content = await (await blob.download_blob()).readall()

        output_path.parent.mkdir(parents=True, exist_ok=True)

        async with aiofiles.open(output_path, "wb") as f:
            await f.write(content)

        if logger:
            logger.info(f"{GREEN}✓ Downloaded to {output_path}{RESET}")

        return True

    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ Download failed: {e}{RESET}")
        return False


async def decompress_gzip(
    input_path: Path,
    output_path: Path,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """Decompress gzip file asynchronously

    Args:
        input_path: Path to gzip file
        output_path: Path to save decompressed file
        logger: Optional logger instance

    Returns:
        True if successful, False otherwise
    """
    try:
        if logger:
            logger.info("🔄 Decompressing gzip file...")

        async with aiofiles.open(input_path, "rb") as f:
            content = await f.read()

        decompressed = gzip.decompress(content)

        async with aiofiles.open(output_path, "wb") as f:
            await f.write(decompressed)

        if logger:
            logger.info(f"{GREEN}✓ Decompressed to {output_path}{RESET}")

        return True

    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ Decompression failed: {e}{RESET}")
        return False


async def csv_to_parquet(
    csv_path: Path,
    parquet_path: Path,
    table_stem: str = "",
    logger: Optional[logging.Logger] = None,
) -> bool:
    """Convert CSV file to Parquet format with optional filtering

    Args:
        csv_path: Path to CSV file
        parquet_path: Path to save Parquet file
        table_stem: Table name stem for special handling (e.g., 'FactInvoiceSecondary')
        logger: Optional logger instance

    Returns:
        True if successful, False otherwise

    Note:
        Special handling for FactInvoiceSecondary:
        - Converts invoicedate to Int32
        - Filters to records after 2023-03-31
    """
    try:
        if logger:
            logger.info(f"🔄 Converting CSV to Parquet: {csv_path}")

        # Scan CSV with lazy loading
        df_stream = pl.scan_csv(
            csv_path,
            infer_schema_length=0,
            null_values=["\x00"],
            ignore_errors=False,
        )

        # Apply table-specific transformations
        if "FactInvoiceSecondary" in table_stem:
            if logger:
                logger.info("  📊 Applying FactInvoiceSecondary filters...")

            df_stream = df_stream.with_columns(pl.col("invoicedate").cast(pl.Int32)).filter(
                pl.col("invoicedate") > 20230331
            )

        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        df_stream.sink_parquet(parquet_path, row_group_size=PARQUET_ROW_GROUP_SIZE)

        if logger:
            logger.info(f"{GREEN}✓ Parquet file saved to {parquet_path}{RESET}")

        return True

    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ CSV to Parquet conversion failed: {e}{RESET}")
        return False


async def process_blob_with_retry(
    blob_path: str,
    container: ContainerClient,
    output_dir: Path,
    table_stem: str = "",
    logger: Optional[logging.Logger] = None,
    attempt: int = 1,
) -> Optional[Path]:
    """Process blob with automatic retry logic

    Downloads blob, optionally decompresses, converts to Parquet

    Args:
        blob_path: Path of blob in Azure container
        container: Azure BlobContainerClient
        output_dir: Directory to save processed files
        table_stem: Table name stem for special handling
        logger: Optional logger instance
        attempt: Current retry attempt (internal)

    Returns:
        Path to processed Parquet file, or None if failed after max retries
    """
    try:
        if logger:
            logger.info(f"📦 Processing blob: {blob_path} (Attempt {attempt})")

        # Prepare paths
        raw_file_path = output_dir / "raw" / blob_path
        parquet_path = output_dir / "cleaned" / f"{clean_file_name(blob_path)}.parquet"

        # Download blob
        success = await download_blob(blob_path, container, raw_file_path, logger)
        if not success:
            raise Exception("Failed to download blob")

        # Handle gzip decompression
        if blob_path.lower().endswith(".gz"):
            decompressed_path = raw_file_path.with_suffix("")
            success = await decompress_gzip(raw_file_path, decompressed_path, logger)
            if not success:
                raise Exception("Failed to decompress file")
            raw_file_path = decompressed_path
            raw_file_path_gz = raw_file_path.parent / f"{raw_file_path.name}.gz"
            if raw_file_path_gz.exists():
                raw_file_path_gz.unlink()

        # Convert CSV to Parquet
        success = await csv_to_parquet(raw_file_path, parquet_path, table_stem, logger)
        if not success:
            raise Exception("Failed to convert CSV to Parquet")

        # Clean up raw file
        if raw_file_path.exists():
            raw_file_path.unlink()

        if logger:
            logger.info(f"{GREEN}✅ Blob processed successfully{RESET}")

        return parquet_path

    except Exception as e:
        if attempt < MAX_RETRIES:
            wait = RETRY_DELAY**attempt
            if logger:
                logger.warning(
                    f"{YELLOW}❌ Processing failed, retrying in {wait:.1f}s "
                    f"(Attempt {attempt}/{MAX_RETRIES})...{RESET}"
                )
            await asyncio.sleep(wait)
            return await process_blob_with_retry(
                blob_path, container, output_dir, table_stem, logger, attempt + 1
            )
        else:
            if logger:
                logger.error(
                    f"{RED}❌ Maximum retries reached for {blob_path}. " f"Final error: {e}{RESET}"
                )
            return None


async def process_blobs_sequentially(
    blob_paths: list,
    container: ContainerClient,
    output_dir: Path,
    logger: Optional[logging.Logger] = None,
) -> dict:
    """Process multiple blobs sequentially

    Args:
        blob_paths: List of blob paths to process
        container: Azure BlobContainerClient
        output_dir: Directory to save processed files
        logger: Optional logger instance

    Returns:
        Dictionary with results:
        {
            'successful': [parquet_path1, ...],
            'failed': [blob_path1, ...],
            'results': {blob_path: parquet_path_or_none}
        }
    """
    results = {
        "successful": [],
        "failed": [],
        "results": {},
    }

    for i, blob_path in enumerate(blob_paths, 1):
        if logger:
            logger.info(f"\n{'='*80}")
            logger.info(f"[{i}/{len(blob_paths)}] Processing: {blob_path}")
            logger.info(f"{'='*80}")

        # Extract table stem from blob path for special handling
        table_stem = blob_path.split("/")[1] if "/" in blob_path else ""

        parquet_path = await process_blob_with_retry(
            blob_path, container, output_dir, table_stem, logger
        )

        if parquet_path:
            results["successful"].append(parquet_path)
            results["results"][blob_path] = parquet_path
        else:
            results["failed"].append(blob_path)
            results["results"][blob_path] = None

    if logger:
        logger.info(f"\n{'='*80}")
        logger.info(
            f"📊 Blob Processing Summary: {len(results['successful'])} successful, "
            f"{len(results['failed'])} failed"
        )
        logger.info(f"{'='*80}")

    return results
