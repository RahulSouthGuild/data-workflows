import os
import shutil
import asyncio
from pathlib import Path
import polars as pl
import time
from typing import List
import sys

from colorama import init

init(autoreset=True)

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.schema_validator import SchemaValidator  # noqa: E402

RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"

# Initialize schema validator with schema files from db/schemas
SCHEMAS_DIR = Path(__file__).parent.parent.parent / "db" / "schemas"
validator = SchemaValidator.from_schema_files(SCHEMAS_DIR)


def get_table_name_from_file(file_stem: str) -> str:
    """Map parquet filename to database table name."""
    if "FactInvoiceSecondary" in file_stem:
        return "FactInvoiceSecondary"
    elif "FactInvoiceDetails" in file_stem:
        return "FactInvoiceDetails"
    elif "DimDealer" in file_stem:
        return "DimDealerMaster"
    else:
        # Try to use stem as-is
        return file_stem


def determine_chunk_size(file_size_mb):
    """Determine chunk size based on file size"""
    if file_size_mb > 1000:  # > 1GB
        return 100000
    elif file_size_mb > 500:  # 500MB - 1GB
        return 250000
    elif file_size_mb > 100:  # 100MB - 500MB
        return 500000
    else:
        return None  # Process entire file


async def process_large_file_chunked(parquet_file, output_dir, chunk_size):
    """Process large files in chunks with optimized memory usage - streaming approach"""
    try:
        start_time = time.time()
        print(f"\n{CYAN}Processing large file in chunks: {parquet_file}{RESET}")

        # Get table name from file
        table_name = get_table_name_from_file(parquet_file.stem)

        # Read file lazily
        lf = pl.scan_parquet(parquet_file)
        total_rows = lf.select(pl.len()).collect().item()

        print(f"{CYAN}Total rows: {total_rows}, Chunk size: {chunk_size}{RESET}")

        # Prepare output path
        output_path = output_dir / parquet_file.name
        if output_path.exists():
            output_path.unlink()

        # Create temporary directory for chunks
        temp_dir = output_dir / f"temp_{parquet_file.stem}_{int(time.time())}"
        temp_dir.mkdir(exist_ok=True)

        try:
            chunk_files = []

            # Process all chunks first
            for i in range(0, total_rows, chunk_size):
                chunk_num = i // chunk_size + 1
                print(
                    f"{CYAN}Processing chunk {chunk_num}/{(total_rows + chunk_size - 1) // chunk_size}{RESET}"
                )

                # Process chunk
                df_chunk = lf.slice(i, chunk_size).collect()

                # Apply validation
                df_chunk = await validate_and_transform_dataframe(df_chunk, table_name)

                # Save chunk to temp directory
                chunk_file = temp_dir / f"chunk_{chunk_num:06d}.parquet"
                df_chunk.write_parquet(chunk_file)
                chunk_files.append(chunk_file)

                # Clear chunk from memory immediately
                del df_chunk

            # Now combine all chunks using lazy evaluation
            print(f"{CYAN}Combining {len(chunk_files)} chunks using streaming{RESET}")

            if chunk_files:
                # Use lazy scanning for memory efficiency
                lazy_frames = [pl.scan_parquet(chunk_file) for chunk_file in chunk_files]
                combined_lf = pl.concat(lazy_frames)

                # Stream write the final result
                combined_lf.sink_parquet(output_path)

            elapsed_time = time.time() - start_time
            print(
                f"{GREEN}Written chunked parquet to {output_path} in {elapsed_time:.2f} seconds{RESET}"
            )

        finally:
            # Clean up temporary directory
            if temp_dir.exists():
                shutil.rmtree(temp_dir)

    except Exception as e:
        print(f"{RED}Error processing chunked file {parquet_file}: {e}{RESET}")
        raise


async def process_large_file_chunked_optimized(parquet_file, output_dir, chunk_size):
    """Most memory-efficient approach with progressive combining"""
    try:
        start_time = time.time()
        print(f"\n{CYAN}Processing large file in chunks (optimized): {parquet_file}{RESET}")

        # Get table name from file
        table_name = get_table_name_from_file(parquet_file.stem)

        # Read file lazily
        lf = pl.scan_parquet(parquet_file)
        total_rows = lf.select(pl.len()).collect().item()

        print(f"{CYAN}Total rows: {total_rows}, Chunk size: {chunk_size}{RESET}")

        # Prepare output path
        output_path = output_dir / parquet_file.name
        if output_path.exists():
            output_path.unlink()

        # Create temporary directory for chunks
        temp_dir = output_dir / f"temp_{parquet_file.stem}_{int(time.time())}"
        temp_dir.mkdir(exist_ok=True)

        try:
            chunk_files = []
            merge_threshold = 5  # Merge every 5 chunks to keep memory low

            # Process chunks with progressive merging
            for i in range(0, total_rows, chunk_size):
                chunk_num = i // chunk_size + 1
                print(
                    f"{CYAN}Processing chunk {chunk_num}/{(total_rows + chunk_size - 1) // chunk_size}{RESET}"
                )

                # Process chunk
                df_chunk = lf.slice(i, chunk_size).collect()

                # Apply validation
                df_chunk = await validate_and_transform_dataframe(df_chunk, table_name)

                # Save chunk to temp directory
                chunk_file = temp_dir / f"chunk_{chunk_num:06d}.parquet"
                df_chunk.write_parquet(chunk_file)
                chunk_files.append(chunk_file)

                # Clear chunk from memory immediately
                del df_chunk

                # Progressive merging to keep chunk count manageable
                if len(chunk_files) >= merge_threshold:
                    print(f"{CYAN}Merging {len(chunk_files)} intermediate chunks{RESET}")
                    merged_file = await _merge_chunks_efficiently(chunk_files, temp_dir)
                    chunk_files = [merged_file]

            # Final combination
            print(f"{CYAN}Final combination of {len(chunk_files)} chunks{RESET}")

            if len(chunk_files) == 1:
                # Just move the single file
                shutil.move(str(chunk_files[0]), str(output_path))
            else:
                # Combine remaining chunks
                lazy_frames = [pl.scan_parquet(chunk_file) for chunk_file in chunk_files]
                combined_lf = pl.concat(lazy_frames)
                combined_lf.sink_parquet(output_path)

            elapsed_time = time.time() - start_time
            print(
                f"{GREEN}Written optimized chunked parquet to {output_path} in {elapsed_time:.2f} seconds{RESET}"
            )

        finally:
            # Clean up temporary directory
            if temp_dir.exists():
                shutil.rmtree(temp_dir)

    except Exception as e:
        print(f"{RED}Error processing optimized chunked file {parquet_file}: {e}{RESET}")
        raise


async def _merge_chunks_efficiently(chunk_files, temp_dir):
    """Efficiently merge chunks using lazy evaluation"""
    if len(chunk_files) <= 1:
        return chunk_files[0] if chunk_files else None

    # Use lazy frames for memory efficiency
    lazy_frames = [pl.scan_parquet(chunk_file) for chunk_file in chunk_files]
    combined_lf = pl.concat(lazy_frames)

    # Create merged file
    merged_file = temp_dir / f"merged_{int(time.time() * 1000000)}.parquet"
    combined_lf.sink_parquet(merged_file)

    # Clean up individual chunks
    for chunk_file in chunk_files:
        try:
            chunk_file.unlink()
        except Exception:
            pass  # Ignore cleanup errors

    return merged_file


def configure_polars_for_low_memory():
    """Configure Polars for memory-efficient operations"""
    # Set smaller streaming chunk size to reduce memory usage
    pl.Config.set_streaming_chunk_size(25000)  # Reduced further

    # Limit thread count to reduce CPU pressure and memory contention
    cpu_count = os.cpu_count()
    pl.Config.set_tbl_cols(50)

    # Use fewer threads to reduce memory pressure
    thread_count = max(1, cpu_count // 4)  # Even more conservative
    os.environ["POLARS_MAX_THREADS"] = str(thread_count)

    # Set memory limit if available
    try:
        pl.Config.set_streaming_engine(True)
    except Exception:
        pass  # Ignore if not available


async def validate_and_transform_dataframe(df: pl.DataFrame, table_name: str) -> pl.DataFrame:
    """
    Validate dataframe against database schema limits and apply type conversions.
    Automatically upgrades schema when data exceeds limits.

    Args:
        df: Polars DataFrame from parquet file
        table_name: Database table name for schema lookup

    Returns:
        Validated and potentially transformed dataframe

    Raises:
        ValueError: If validation fails critically
    """
    print(f"{CYAN}Validating data against table schema: {table_name}{RESET}")

    # The validator now returns validation status and potentially modified dataframe
    is_valid, error_msg, transformed_df = validator.validate_dataframe_against_schema(
        df, table_name
    )

    if not is_valid:
        error_msg = f"{RED}Validation Error for {table_name}: {error_msg}{RESET}"
        print(error_msg)
        raise ValueError(error_msg)

    print(f"{GREEN}Validation passed for {table_name}{RESET}")
    return transformed_df


async def process_parquet_file(parquet_file, output_dir):
    try:
        start_time = time.time()
        print(parquet_file)
        print(f"\n{CYAN}Processing file: {parquet_file}{RESET}")

        # Get table name from file
        table_name = get_table_name_from_file(parquet_file.stem)

        # Check file size for chunking decision
        file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        chunk_size = determine_chunk_size(file_size_mb)

        if chunk_size:
            print(
                f"{CYAN}File size: {file_size_mb:.2f}MB - Using optimized chunked processing{RESET}"
            )
            # Use the optimized version instead
            await process_large_file_chunked_optimized(parquet_file, output_dir, chunk_size)
            return

        # Regular processing for smaller files
        print(f"{CYAN}File size: {file_size_mb:.2f}MB - Using regular processing{RESET}")

        df = pl.read_parquet(parquet_file)
        print(f"{GREEN}Read {parquet_file} successfully{RESET}")

        # Apply validation
        df = await validate_and_transform_dataframe(df, table_name)

        output_path = output_dir / parquet_file.name
        if output_path.exists():
            output_path.unlink()
        df.write_parquet(output_path)

        elapsed_time = time.time() - start_time
        print(
            f"{GREEN}Written cleaned parquet to {output_path} in {elapsed_time:.2f} seconds{RESET}"
        )
    except Exception as e:
        print(f"{RED}Error processing {parquet_file}: {e}{RESET}")
        raise


async def process_batch(files, output_dir, semaphore):
    async with semaphore:
        tasks = []
        for file in files:
            task = asyncio.create_task(process_parquet_file(file, output_dir))
            tasks.append(task)
        return await asyncio.gather(*tasks, return_exceptions=True)


async def display_file_menu(files: List[Path]) -> int:
    print("\nAvailable files:")
    for idx, file in enumerate(files, 1):
        print(f"{idx}. {file.name}")
    print("0. Exit")

    while True:
        try:
            choice = int(input("\nEnter file number to process (0 to exit): "))
            if 0 <= choice <= len(files):
                return choice
            print(f"{RED}Invalid choice. Please enter a number between 0 and {len(files)}{RESET}")
        except ValueError:
            print(f"{RED}Please enter a valid number{RESET}")


async def main():
    start_time = time.time()
    try:
        # Configure Polars for memory efficiency
        configure_polars_for_low_memory()

        base_dir = PROJECT_ROOT / "data" / "data_historical"
        raw_dir = base_dir / "raw_parquets"
        clean_dir = base_dir / "cleaned_parquets"
        if clean_dir.exists():
            shutil.rmtree(clean_dir)
        print(f"{CYAN}Creating necessary directories{RESET}")
        os.makedirs(clean_dir, exist_ok=True)

        print(
            f"{CYAN}Schema validator initialized with {len(validator.tables)} tables from tables.py{RESET}"
        )

        files_to_process = list(raw_dir.glob("**/*.parquet"))
        total_files = len(files_to_process)

        while True:
            print(f"\n{CYAN}Menu Options:{RESET}")
            print("1. Process all files")
            print("2. Select specific file")
            print("3. Exit")

            try:
                choice = int(input("\nEnter your choice (1-3): "))

                if choice == 1:
                    print(f"{CYAN}Found {total_files} files to process{RESET}")
                    # Process all files
                    chunk_size = 1
                    max_concurrent_tasks = 1
                    semaphore = asyncio.Semaphore(max_concurrent_tasks)

                    for i in range(0, total_files, chunk_size):
                        chunk = files_to_process[i : i + chunk_size]
                        print(
                            f"{CYAN}Processing chunk {i // chunk_size + 1}/{(total_files + chunk_size - 1) // chunk_size}{RESET}"
                        )
                        results = await process_batch(chunk, clean_dir, semaphore)

                        for result, file in zip(results, chunk):
                            if isinstance(result, Exception):
                                print(f"{RED}Error processing {file}: {result}{RESET}")

                elif choice == 2:
                    file_choice = await display_file_menu(files_to_process)
                    if file_choice == 0:
                        continue

                    selected_file = files_to_process[file_choice - 1]
                    semaphore = asyncio.Semaphore(1)
                    await process_parquet_file(selected_file, clean_dir)

                elif choice == 3:
                    print(f"{GREEN}Exiting program{RESET}")
                    sys.exit(0)

                else:
                    print(f"{RED}Invalid choice. Please enter 1, 2, or 3{RESET}")

            except ValueError:
                print(f"{RED}Please enter a valid number{RESET}")

    except Exception as e:
        print(f"{RED}Error in main: {e}{RESET}")
    finally:
        # Display schema change summary
        try:
            summary = validator.get_schema_change_summary()
            if summary:
                print(f"\n{YELLOW}{'='*80}{RESET}")
                print(f"{YELLOW}SCHEMA CHANGES SUMMARY{RESET}")
                print(f"{YELLOW}{'='*80}{RESET}")
                print(summary)
                print(f"{YELLOW}{'='*80}{RESET}\n")
        except Exception as e:
            print(f"{RED}Error displaying schema summary: {e}{RESET}")

        # Display and save ALTER TABLE statements
        try:
            alter_stmts = validator.get_alter_table_statements()
            if alter_stmts:
                validator.print_alter_statements()
                validator.save_alter_statements_to_file()
        except Exception as e:
            print(f"{RED}Error handling ALTER statements: {e}{RESET}")

        end_time = time.time()
        print(f"{GREEN}Operation time: {end_time - start_time:.2f} seconds{RESET}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"{RED}Unhandled error: {e}{RESET}")
