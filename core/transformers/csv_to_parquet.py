from pathlib import Path
import re
import shutil
import polars as pl
import asyncio
import time
from colorama import Fore, Style, init

init(autoreset=True)


PROJECT_ROOT = Path(__file__).parent.parent.parent


def get_size_format(bytes):
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes < 1024:
            return f"{bytes:.2f}{unit}"
        bytes /= 1024
    return f"{bytes:.2f}GB"


async def process_single_file(path):
    start_time = time.time()
    try:
        output_dir = PROJECT_ROOT / "data" / "data_historical" / "raw_parquets"
        output_dir.mkdir(exist_ok=True, parents=True)

        file_name = path.stem
        if "FactInvoiceDetails" in file_name:
            match = re.match(r"(FactInvoiceDetails(?:_\d+){1,2})_", file_name)
            cleaned_file_name = match.group(1) if match else file_name.split("_")[0]
        elif "FactInvoiceSecondary" in file_name:
            match = re.match(r"(FactInvoiceSecondary(?:_\d+){1,2})_", file_name)
            cleaned_file_name = match.group(1) if match else file_name.split("_")[0]
        else:
            cleaned_file_name = re.sub(r"_.*", "", file_name)

        if cleaned_file_name.lower() == "dimdealer":
            cleaned_file_name = "DimDealerMaster"

        # Convert to snake case
        parquet_file = output_dir / f"{cleaned_file_name}.parquet"

        if parquet_file.exists():
            await asyncio.to_thread(parquet_file.unlink)

        raw_size = path.stat().st_size
        if raw_size == 0:
            print(f"{Fore.YELLOW}Skipping empty file: {path}{Style.RESET_ALL}")
            return

        for attempt in range(3):
            try:
                print(f"{Fore.CYAN}Processing {path.name} - Attempt {attempt + 1}{Style.RESET_ALL}")
                df_stream = pl.scan_csv(
                    path,
                    schema_overrides={"": pl.Utf8},
                    infer_schema_length=0,
                    rechunk=True,
                    low_memory=False,
                )

                if "FactInvoiceSecondary" in cleaned_file_name:
                    df_stream = df_stream.with_columns(pl.col("invoicedate").cast(pl.Int32)).filter(
                        pl.col("invoicedate") > 20230331
                    )

                elif "FactInvoiceDetails" in cleaned_file_name:
                    df_stream = df_stream.with_columns(pl.col("postingdate").cast(pl.Int32)).filter(
                        pl.col("postingdate") > 20230331
                    )

                await asyncio.to_thread(df_stream.sink_parquet, parquet_file, row_group_size=100000)

                new_size = parquet_file.stat().st_size
                compression_ratio = (1 - new_size / raw_size) * 100
                elapsed = time.time() - start_time
                print(
                    f"{Fore.GREEN}Processed {path.name} → {parquet_file.name} in {elapsed:.2f}s"
                    f"\nSize: {get_size_format(raw_size)} → {get_size_format(new_size)} "
                    f"({compression_ratio:.1f}% reduction){Style.RESET_ALL}"
                )
                return
            except Exception as e:
                print(f"{Fore.RED}Attempt {attempt + 1} failed: {str(e)}{Style.RESET_ALL}")
                if attempt < 2:
                    await asyncio.sleep(2)

        raise Exception(f"Failed to process {path.name} after 3 attempts")

    except Exception as e:
        print(f"{Fore.RED}Error processing {path.name}: {str(e)}{Style.RESET_ALL}")
        raise


def display_menu():
    print(f"{Fore.CYAN}=" * 50)
    print(f"{Fore.CYAN}CSV to Parquet Converter")
    print(f"{Fore.CYAN}=" * 50)
    print(f"{Fore.GREEN}1. Convert all files")
    print(f"{Fore.GREEN}2. Convert missing files only")
    print(f"{Fore.GREEN}3. Select specific files to convert")
    print(f"{Fore.GREEN}4. Exit")
    print(f"{Fore.CYAN}=" * 50)
    return input(f"{Fore.YELLOW}Select an option (1-4): {Style.RESET_ALL}")


async def select_files_to_convert():
    try:
        csv_files = list(
            (PROJECT_ROOT / "data" / "data_historical" / "raw").glob("**/*.csv")
        )
        if not csv_files:
            print(
                f"{Fore.RED}No CSV files found in data/data_historical/raw{Style.RESET_ALL}"
            )
            return []

        print(f"{Fore.CYAN}Available files:{Style.RESET_ALL}")
        for i, file_path in enumerate(csv_files):
            print(f"{Fore.GREEN}[{i + 1}] {file_path.name}{Style.RESET_ALL}")

        selection = input(
            f"{Fore.YELLOW}Enter file numbers to convert (comma separated, e.g. 1,3,5) or 'all': {Style.RESET_ALL}"
        )
        if selection.lower() == "all":
            return csv_files

        try:
            selected_indices = [int(idx.strip()) - 1 for idx in selection.split(",")]
            return [csv_files[i] for i in selected_indices if 0 <= i < len(csv_files)]
        except (ValueError, IndexError):
            print(f"{Fore.RED}Invalid selection. Please enter valid numbers.{Style.RESET_ALL}")
            return await select_files_to_convert()
    except Exception as e:
        print(f"{Fore.RED}Error while selecting files: {str(e)}{Style.RESET_ALL}")
        return []


async def find_missing_files():
    try:
        csv_files = list(
            (PROJECT_ROOT / "data" / "data_historical" / "raw").glob("**/*.csv")
        )
        output_dir = PROJECT_ROOT / "data" / "data_historical" / "raw_parquets"
        output_dir.mkdir(exist_ok=True, parents=True)

        missing_files = []
        for path in csv_files:
            file_name = path.stem
            if "FactInvoiceDetails" in file_name:
                match = re.match(r"(FactInvoiceDetails(?:_\d+){1,2})_", file_name)
                cleaned_file_name = match.group(1) if match else file_name.split("_")[0]
            elif "FactInvoiceSecondary" in file_name:
                cleaned_file_name = re.sub(r"(FactInvoiceSecondary_\d+).*", r"\1", file_name)
            else:
                cleaned_file_name = re.sub(r"_.*", "", file_name)

            if cleaned_file_name.lower() == "dimdealer":
                cleaned_file_name = "DimDealerMaster"
            parquet_file = output_dir / f"{cleaned_file_name}.parquet"
            if not parquet_file.exists():
                missing_files.append(path)

        print(f"{Fore.CYAN}Found {len(missing_files)} missing parquet files{Style.RESET_ALL}")
        return missing_files
    except Exception as e:
        print(f"{Fore.RED}Error finding missing files: {str(e)}{Style.RESET_ALL}")
        return []


async def process_files(files_to_process):
    start_time = time.time()
    try:
        print(f"{Fore.CYAN}Processing {len(files_to_process)} files{Style.RESET_ALL}")
        for file_path in files_to_process:
            await process_single_file(file_path)

        total_time = time.time() - start_time
        print(
            f"{Fore.GREEN}Completed {len(files_to_process)} files in {total_time:.2f}s{Style.RESET_ALL}"
        )
    except Exception as e:
        print(f"{Fore.RED}Processing failed: {str(e)}{Style.RESET_ALL}")
        raise


async def main():
    while True:
        choice = display_menu()

        if choice == "1":
            try:

                raw_parquets_dir = PROJECT_ROOT / "data" / "data_historical" / "raw_parquets"
                if raw_parquets_dir.exists():
                    await asyncio.to_thread(shutil.rmtree, raw_parquets_dir)

                csv_files = list(
                    (PROJECT_ROOT / "data" / "data_historical" / "raw").glob("**/*.csv")
                )
                print(f"{Fore.CYAN}Found {len(csv_files)} CSV files to process{Style.RESET_ALL}")
                await process_files(csv_files)
            except Exception as e:
                print(f"{Fore.RED}Error processing all files: {str(e)}{Style.RESET_ALL}")

        elif choice == "2":
            try:
                missing_files = await find_missing_files()
                if missing_files:
                    await process_files(missing_files)
                else:
                    print(f"{Fore.YELLOW}No missing files to process{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}Error processing missing files: {str(e)}{Style.RESET_ALL}")

        elif choice == "3":
            try:
                selected_files = await select_files_to_convert()
                if selected_files:
                    await process_files(selected_files)
                else:
                    print(f"{Fore.YELLOW}No files selected{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}Error processing selected files: {str(e)}{Style.RESET_ALL}")

        elif choice == "4":
            print(f"{Fore.CYAN}Exiting...{Style.RESET_ALL}")
            break

        else:
            print(f"{Fore.RED}Invalid option. Please select 1-4.{Style.RESET_ALL}")

        print()


if __name__ == "__main__":
    asyncio.run(main())
