import os
import json
import gzip
import time
import shutil
from datetime import datetime, timedelta

# --- Configuration ---
# Define paths for our simulated storage tiers
HOT_TIER_PATH = 'data/hot'
COOL_TIER_PATH = 'data/cool'
ARCHIVE_TIER_PATH = 'data/archive'
REHYDRATED_TIER_PATH = 'data/rehydrated' # For rehydrated archive data

# Data retention policies (simulated in months)
HOT_TIER_RETENTION_MONTHS = 3
COOL_TIER_RETENTION_MONTHS = 12 # Total age for cool, so data older than 3 up to 12 months

# Compression levels for gzip (0-9, 9 is highest)
COOL_COMPRESSION_LEVEL = 5
ARCHIVE_COMPRESSION_LEVEL = 9

# --- Helper Functions ---

def setup_directories():
    """Ensures all necessary data directories exist."""
    for path in [HOT_TIER_PATH, COOL_TIER_PATH, ARCHIVE_TIER_PATH, REHYDRATED_TIER_PATH]:
        os.makedirs(path, exist_ok=True)
    print("Directories set up successfully.")

def generate_billing_record(record_id: int, date: datetime) -> dict:
    """Generates a dummy billing record."""
    return {
        "record_id": f"BILL-{record_id:05d}",
        "customer_id": f"CUST-{record_id % 100 + 1:03d}",
        "invoice_date": date.isoformat(),
        "amount": round(record_id * 1.23 + 100, 2),
        "currency": "USD",
        "description": f"Service usage for period ending {date.strftime('%Y-%m-%d')}",
        "line_items": [
            {"item": "Compute", "qty": record_id % 5 + 1, "price": round(20.5 + record_id % 3, 2)},
            {"item": "Storage", "qty": record_id % 10 + 1, "price": round(5.1 + record_id % 2, 2)}
        ]
    }

def get_file_path(tier_path: str, filename: str, compressed: bool = False) -> str:
    """Constructs the full path for a file in a given tier."""
    if compressed:
        return os.path.join(tier_path, f"{filename}.gz")
    return os.path.join(tier_path, filename)

def write_data(file_path: str, data: dict, compress_level: int = None):
    """Writes data to a file, with optional gzip compression."""
    try:
        json_data = json.dumps(data, indent=2).encode('utf-8')
        if compress_level is not None:
            with gzip.open(file_path, 'wb', compresslevel=compress_level) as f:
                f.write(json_data)
            print(f"  --> Data written (compressed) to: {file_path}")
        else:
            with open(file_path, 'wb') as f:
                f.write(json_data)
            print(f"  --> Data written (uncompressed) to: {file_path}")
    except Exception as e:
        print(f"Error writing data to {file_path}: {e}")

def read_data(file_path: str, compressed: bool = False) -> dict | None:
    """Reads data from a file, with optional gzip decompression."""
    try:
        if compressed:
            with gzip.open(file_path, 'rb') as f:
                content = f.read()
        else:
            with open(file_path, 'rb') as f:
                content = f.read()
        return json.loads(content.decode('utf-8'))
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return None
    except Exception as e:
        print(f"Error reading/decompressing data from {file_path}: {e}")
        return None

# --- Core Logic Functions ---

def ingest_data(num_records: int, days_ago: int = 0):
    """Simulates ingesting new billing records into the Hot Tier."""
    print(f"\n--- Ingesting {num_records} new records to Hot Tier ---")
    current_date = datetime.now() - timedelta(days=days_ago)
    for i in range(num_records):
        record = generate_billing_record(i + 1, current_date)
        filename = f"{record['record_id']}.json"
        write_data(get_file_path(HOT_TIER_PATH, filename), record)

def manage_data_tiers():
    """
    Automates the movement of data between tiers based on age and applies compression.
    Simulates a daily/monthly scheduled job.
    """
    print("\n--- Running Tier Management Process ---")
    now = datetime.now()

    # 1. Hot Tier to Cool Tier Movement
    print("\nProcessing Hot Tier for Cool Tier transfer...")
    for filename in os.listdir(HOT_TIER_PATH):
        if not filename.endswith('.json'):
            continue

        file_path = get_file_path(HOT_TIER_PATH, filename)
        try:
            # Assuming invoice_date represents the age for simplicity
            # In a real system, you might use file creation/modification date or a metadata field
            record_data = read_data(file_path)
            if not record_data or 'invoice_date' not in record_data:
                print(f"  Skipping {filename}: Invalid record data or missing invoice_date.")
                continue

            invoice_date = datetime.fromisoformat(record_data['invoice_date'])
            age_in_months = (now - invoice_date).days / 30.44

            if age_in_months >= HOT_TIER_RETENTION_MONTHS:
                print(f"  Moving {filename} (age: {age_in_months:.1f} months) from Hot to Cool Tier...")
                cool_file_path = get_file_path(COOL_TIER_PATH, filename, compressed=True)
                write_data(cool_file_path, record_data, compress_level=COOL_COMPRESSION_LEVEL)
                os.remove(file_path) # Delete from hot tier after successful move
                print(f"  -> Successfully moved {filename} to Cool Tier (compressed).")
            else:
                print(f"  {filename} (age: {age_in_months:.1f} months) remains in Hot Tier.")

        except Exception as e:
            print(f"  Error processing {filename} for Hot->Cool transfer: {e}")

    # 2. Cool Tier to Archive Tier Movement
    print("\nProcessing Cool Tier for Archive Tier transfer...")
    for filename_gz in os.listdir(COOL_TIER_PATH):
        if not filename_gz.endswith('.json.gz'):
            continue

        original_filename = filename_gz.replace('.gz', '')
        file_path_cool = get_file_path(COOL_TIER_PATH, original_filename, compressed=True)
        try:
            # To get original invoice_date, we need to read and decompress first
            record_data = read_data(file_path_cool, compressed=True)
            if not record_data or 'invoice_date' not in record_data:
                print(f"  Skipping {filename_gz}: Invalid record data or missing invoice_date.")
                continue

            invoice_date = datetime.fromisoformat(record_data['invoice_date'])
            age_in_months = (now - invoice_date).days / 30.44

            if age_in_months >= COOL_TIER_RETENTION_MONTHS:
                print(f"  Moving {original_filename} (age: {age_in_months:.1f} months) from Cool to Archive Tier...")
                archive_file_path = get_file_path(ARCHIVE_TIER_PATH, original_filename, compressed=True)
                write_data(archive_file_path, record_data, compress_level=ARCHIVE_COMPRESSION_LEVEL)
                os.remove(file_path_cool) # Delete from cool tier after successful move
                print(f"  -> Successfully moved {original_filename} to Archive Tier (highly compressed).")
            else:
                print(f"  {original_filename} (age: {age_in_months:.1f} months) remains in Cool Tier.")

        except Exception as e:
            print(f"  Error processing {filename_gz} for Cool->Archive transfer: {e}")

    print("\nTier management process completed.")

def retrieve_data(record_id: str, high_priority: bool = False) -> dict | None:
    """
    Simulates an intelligent data retrieval process from any tier.
    For Archive tier, it simulates rehydration.
    """
    print(f"\n--- Attempting to retrieve record: {record_id} ---")
    filename = f"{record_id}.json"

    # 1. Check Hot Tier
    file_path_hot = get_file_path(HOT_TIER_PATH, filename)
    if os.path.exists(file_path_hot):
        print(f"  Found '{record_id}' in Hot Tier. Retrieving immediately.")
        data = read_data(file_path_hot)
        return data

    # 2. Check Cool Tier
    file_path_cool = get_file_path(COOL_TIER_PATH, filename, compressed=True)
    if os.path.exists(file_path_cool):
        print(f"  Found '{record_id}' in Cool Tier (compressed). Decompressing and retrieving...")
        data = read_data(file_path_cool, compressed=True)
        return data

    # 3. Check Archive Tier (requires rehydration simulation)
    file_path_archive = get_file_path(ARCHIVE_TIER_PATH, filename, compressed=True)
    if os.path.exists(file_path_archive):
        print(f"  Found '{record_id}' in Archive Tier.")
        print(f"  --- Initiating REHYDRATION Process for '{record_id}' ---")
        if high_priority:
            print("  (High Priority Retrieval requested - simulated faster, but higher cost)")
            retrieval_time_sec = 5 # Simulate faster retrieval
        else:
            print("  (Standard Retrieval - will take longer)")
            retrieval_time_sec = 15 # Simulate longer retrieval (e.g., 15 seconds for 15 hours)

        print(f"  Simulating retrieval and rehydration (approx. {retrieval_time_sec} seconds)...")
        time.sleep(retrieval_time_sec) # Simulate the latency

        rehydrated_path = os.path.join(REHYDRATED_TIER_PATH, filename)
        try:
            # Simulate moving/copying to a temporary hot-accessible location and decompressing
            shutil.copyfile(file_path_archive, f"{rehydrated_path}.gz") # Copy compressed
            data = read_data(f"{rehydrated_path}.gz", compressed=True) # Read and decompress
            os.remove(f"{rehydrated_path}.gz") # Clean up temp compressed file after reading
            if data:
                write_data(rehydrated_path, data) # Write decompressed to rehydrated folder for access
                print(f"  --- REHYDRATION COMPLETE for '{record_id}'. Data available at '{rehydrated_path}' ---")
                return data
            else:
                print(f"  Rehydration failed for {record_id}.")
                return None
        except Exception as e:
            print(f"  Error during rehydration for {record_id}: {e}")
            return None
    
    print(f"  Record '{record_id}' not found in any tier.")
    return None

def show_tier_contents():
    """Displays the current contents of each tier."""
    print("\n--- Current Tier Contents ---")
    print(f"\nHot Tier ({HOT_TIER_PATH}):")
    if not os.listdir(HOT_TIER_PATH):
        print("  (Empty)")
    for f in os.listdir(HOT_TIER_PATH):
        print(f"  - {f} (Uncompressed)")

    print(f"\nCool Tier ({COOL_TIER_PATH}):")
    if not os.listdir(COOL_TIER_PATH):
        print("  (Empty)")
    for f in os.listdir(COOL_TIER_PATH):
        print(f"  - {f} (Gzip Compressed, Level {COOL_COMPRESSION_LEVEL})")

    print(f"\nArchive Tier ({ARCHIVE_TIER_PATH}):")
    if not os.listdir(ARCHIVE_TIER_PATH):
        print("  (Empty)")
    for f in os.listdir(ARCHIVE_TIER_PATH):
        print(f"  - {f} (Gzip Compressed, Level {ARCHIVE_COMPRESSION_LEVEL})")

    print(f"\nRehydrated Tier (Temporary - {REHYDRATED_TIER_PATH}):")
    if not os.listdir(REHYDRATED_TIER_PATH):
        print("  (Empty)")
    for f in os.listdir(REHYDRATED_TIER_PATH):
        print(f"  - {f} (Decompressed from Archive)")

def cleanup_data_dirs():
    """Removes all simulated data directories."""
    print("\n--- Cleaning up data directories ---")
    for path in [HOT_TIER_PATH, COOL_TIER_PATH, ARCHIVE_TIER_PATH, REHYDRATED_TIER_PATH]:
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"Removed: {path}")
    print("Cleanup complete.")

# --- Main Application Loop ---

def main():
    """Main function to run the simulation."""
    setup_directories()

    while True:
        print("\n==============================================")
        print("  Azure Billing Records Cost Optimization Demo")
        print("==============================================")
        print("1. Ingest New Billing Records (Hot Tier)")
        print("2. Run Tier Management (Move data based on age & compress)")
        print("3. Retrieve a Billing Record")
        print("4. Show Current Tier Contents")
        print("5. Clean Up All Data")
        print("6. Exit")
        print("----------------------------------------------")

        choice = input("Enter your choice: ")

        if choice == '1':
            try:
                num = int(input("How many records to ingest? "))
                days = int(input("How many days ago should these records be dated? (e.g., 0 for today, 60 for 2 months ago): "))
                ingest_data(num, days_ago=days)
            except ValueError:
                print("Invalid input. Please enter numbers.")
        elif choice == '2':
            manage_data_tiers()
        elif choice == '3':
            record_id = input("Enter the record ID to retrieve (e.g., BILL-00001): ").strip().upper()
            if not record_id.startswith("BILL-") or not record_id[5:].isdigit():
                print("Invalid record ID format. Please use BILL-XXXXX (e.g., BILL-00001).")
                continue
            
            hp_choice = input("High priority retrieval? (yes/no): ").strip().lower()
            high_priority = (hp_choice == 'yes')
            
            retrieved_record = retrieve_data(record_id, high_priority=high_priority)
            if retrieved_record:
                print("\n--- Retrieved Record Details ---")
                print(json.dumps(retrieved_record, indent=2))
            else:
                print(f"Could not retrieve record {record_id}.")
        elif choice == '4':
            show_tier_contents()
        elif choice == '5':
            confirm = input("Are you sure you want to delete all data? (yes/no): ")
            if confirm.lower() == 'yes':
                cleanup_data_dirs()
            else:
                print("Cleanup cancelled.")
        elif choice == '6':
            print("Exiting. Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
