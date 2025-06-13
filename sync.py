import os
import sys
import json
import time
import logging
import requests
import pyodbc
import gzip
import threading
from datetime import datetime, date
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed


# Custom JSON encoder to handle Decimal objects and date objects
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        return super(DecimalEncoder, self).default(obj)


# Setup logging with better formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[
        logging.FileHandler('sync.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

CONFIG_FILE = 'config.json'


def print_header():
    """Print a nice header for the application"""
    print("\n" + "=" * 70)
    print("          🚀 OPTIMIZED SCHOOL DATABASE SYNC TOOL 🚀")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")


def print_progress_bar(current, total, prefix='Progress', bar_length=40):
    """Print a progress bar"""
    percent = float(current) * 100 / total
    filled_length = int(bar_length * current // total)
    bar = '█' * filled_length + '-' * (bar_length - filled_length)
    print(f'\r{prefix}: |{bar}| {percent:.1f}% ({current}/{total})',
          end='', flush=True)
    if current == total:
        print()


def load_config():
    """Load configuration from config.json file"""
    try:
        print("📋 Loading configuration file...")
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        print("✅ Configuration loaded successfully\n")
        return config
    except FileNotFoundError:
        print(f"❌ ERROR: Configuration file '{CONFIG_FILE}' not found!")
        input("\nPress Enter to exit...")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"❌ ERROR: Invalid JSON format in '{CONFIG_FILE}'!")
        input("\nPress Enter to exit...")
        sys.exit(1)


def connect_to_database(config):
    """Connect to SQL Anywhere database using ODBC"""
    try:
        print("🔌 Connecting to database...")
        dsn = config['database']['dsn']
        username = config['database']['username']
        password = config['database']['password']

        print(f"   → DSN: {dsn}")
        print(f"   → User: {username}")

        conn_str = f"DSN={dsn};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str)
        print("✅ Database connection successful!\n")
        return conn
    except pyodbc.Error as e:
        print(f"❌ Database connection failed: {e}")
        input("\nPress Enter to exit...")
        sys.exit(1)


def execute_query_with_progress(conn, query, table_name):
    """Execute SQL query with progress indication for large datasets"""
    try:
        cursor = conn.cursor()

        # Get total count first for progress tracking
        count_query = f"SELECT COUNT(*) FROM ({query}) AS count_subquery"
        cursor.execute(count_query)
        total_count = cursor.fetchone()[0]

        print(f"   → Fetching {total_count:,} records from {table_name}...")

        # Execute main query
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]

        results = []
        batch_size = 5000  # Process in larger batches

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            batch_results = [dict(zip(columns, row)) for row in rows]
            results.extend(batch_results)

            # Show progress for large datasets
            if total_count > 10000:
                print_progress_bar(len(results), total_count,
                                   f"   Loading {table_name}")

        if total_count > 10000:
            print()  # New line after progress bar

        cursor.close()
        return results

    except pyodbc.Error as e:
        print(f"❌ Query execution failed for {table_name}: {e}")
        return []


def fetch_data_parallel(conn):
    """Fetch data from all required tables using parallel processing"""
    print("📊 FETCHING DATA FROM DATABASE (OPTIMIZED)")
    print("-" * 50)

    # Queries to fetch school data tables
    tables = [
        ("acc_users", 'SELECT "id", "pass" FROM "acc_users"'),
        ("personel", 'SELECT "admission", "name" FROM "personel"'),
        ("mag_subject", 'SELECT "code", "name" FROM "mag_subject"'),
        ("cce_assessmentitems", 'SELECT "code", "name" FROM "cce_assessmentitems"'),
        ("cce_entry", '''SELECT "slno", "admission", "class", "division", "subject", 
                        "assessmentitem", "term", "part", "yearcode", "edate", "mark", 
                        "teacher", "sortorder", "maxmark", "subperiod", "indicator", 
                        "element", "grade", "groupmark", "groupper", "particulars", 
                        "elementgrade", "longdescription" FROM "cce_entry"'''),
    ]

    data = {}
    total_records = 0

    # For small datasets, use sequential processing
    # For large datasets, this could be parallelized, but database connections are limited
    for i, (table_name, query) in enumerate(tables, 1):
        print(f"{i}. Processing {table_name}...")

        results = execute_query_with_progress(conn, query, table_name)

        # Handle special field mappings
        if table_name == "acc_users":
            for record in results:
                if 'pass' in record:
                    record['pass_field'] = record.pop('pass')

        data[table_name] = results
        record_count = len(results)
        total_records += record_count

        print(f"   ✅ {table_name}: {record_count:,} records loaded")

    print("-" * 50)
    print(f"📈 TOTAL RECORDS TO SYNC: {total_records:,}")
    print()

    return data


def reset_sync_session(config):
    """Reset the sync session on the API server"""
    try:
        api_base_url = config['api']['url']
        reset_endpoint = f"{api_base_url}/api/reset-sync-session/"

        response = requests.post(reset_endpoint,
                                 headers={'Content-Type': 'application/json'},
                                 timeout=30)

        if response.status_code == 200:
            print("✅ Sync session reset on server")
            return True
        else:
            print("⚠️  Warning: Could not reset sync session on server")
            return False
    except Exception as e:
        print(f"⚠️  Warning: Could not reset sync session: {e}")
        return False


def sync_data_bulk_optimized(data, config):
    """Optimized bulk sync - send all data in one request"""
    try:
        api_base_url = config['api']['url']
        print(f"🌐 API Server: {api_base_url}")
        print()

        # Reset sync session
        reset_sync_session(config)

        # Use the new bulk sync endpoint
        bulk_sync_endpoint = f"{api_base_url}/api/bulk-sync/"

        # Prepare headers with compression support
        headers = {
            'Content-Type': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        }

        print("📤 PREPARING BULK SYNC PAYLOAD")
        print("-" * 50)

        # Calculate total records
        total_records = sum(len(table_data) for table_data in data.values())
        print(f"📊 Total records to sync: {total_records:,}")

        # Prepare complete payload
        payload = {
            "database": config.get('target_database', 'safa'),
            "tables": data,
            "total_records": total_records,
            "sync_timestamp": datetime.now().isoformat()
        }

        print("🔄 Serializing data to JSON...")
        json_data = json.dumps(payload, cls=DecimalEncoder)
        data_size_mb = len(json_data.encode('utf-8')) / (1024 * 1024)
        print(f"📦 Payload size: {data_size_mb:.2f} MB")

        print("\n📤 SENDING BULK SYNC REQUEST")
        print("-" * 50)
        print("⏳ Uploading data to server...")

        # Send request with longer timeout for large datasets
        # Dynamic timeout based on data size
        timeout = max(300, total_records // 1000 * 10)

        start_time = time.time()

        try:
            response = requests.post(
                bulk_sync_endpoint,
                data=json_data,
                headers=headers,
                timeout=timeout
            )

            upload_time = time.time() - start_time
            print(f"⏱️  Upload completed in {upload_time:.2f} seconds")

            if response.status_code == 200:
                response_data = response.json()
                if response_data.get('success', False):
                    print("✅ BULK SYNC SUCCESSFUL!")
                    print(
                        f"📊 Total records processed: {response_data.get('total_records', 0):,}")
                    print(
                        f"📋 Tables processed: {response_data.get('tables_processed', 0)}")

                    # Show detailed results
                    results = response_data.get('results', {})
                    print("\n📈 DETAILED RESULTS:")
                    print("-" * 30)
                    for table_name, table_result in results.items():
                        records = table_result.get('records_processed', 0)
                        print(f"  {table_name}: {records:,} records")

                    return True
                else:
                    print(
                        f"❌ API Error: {response_data.get('error', 'Unknown error')}")
                    if 'validation_errors' in response_data:
                        print("📋 Validation errors:")
                        for error in response_data['validation_errors'][:5]:
                            print(
                                f"  Row {error.get('row', '?')}: {error.get('errors', {})}")
                    return False
            else:
                print(f"❌ HTTP Error: {response.status_code}")
                try:
                    error_data = response.json()
                    print(f"📋 Error details: {error_data}")
                except:
                    print(f"📋 Response text: {response.text[:500]}...")
                return False

        except requests.exceptions.Timeout:
            print(f"⏰ Request timed out after {timeout} seconds")
            print("💡 Suggestion: Try increasing timeout or reducing batch size")
            return False
        except requests.exceptions.RequestException as e:
            print(f"🌐 Network error: {str(e)}")
            return False

    except Exception as e:
        print(f"❌ Sync Error: {str(e)}")
        return False


def sync_data_to_api_legacy(data, config):
    """Legacy batch sync method - kept as fallback"""
    try:
        api_base_url = config['api']['url']
        print(f"🌐 API Server: {api_base_url} (Legacy Mode)")
        print()

        reset_sync_session(config)

        headers = {'Content-Type': 'application/json'}
        sync_endpoint = f"{api_base_url}/api/sync/"

        # Larger batch size for better performance
        BATCH_SIZE = 3000

        tables_to_sync = ["acc_users", "personel",
                          "mag_subject", "cce_assessmentitems", "cce_entry"]

        print("📤 SYNCING DATA TO API (LEGACY MODE)")
        print("-" * 50)

        def chunk_data(data_list, chunk_size=BATCH_SIZE):
            for i in range(0, len(data_list), chunk_size):
                yield data_list[i:i + chunk_size]

        for table_index, table_name in enumerate(tables_to_sync, 1):
            if table_name in data:
                table_data = data[table_name]
                if not table_data:
                    print(f"{table_index}. {table_name}: No data to sync")
                    continue

                print(
                    f"{table_index}. Syncing {len(table_data):,} records from {table_name}...")

                chunks = list(chunk_data(table_data, chunk_size=BATCH_SIZE))

                for chunk_index, chunk in enumerate(chunks, 1):
                    print_progress_bar(
                        chunk_index - 1, len(chunks), f"   Batch {chunk_index}/{len(chunks)}")

                    is_first_batch = (chunk_index == 1)
                    is_last_batch = (chunk_index == len(chunks))

                    payload = {
                        "database": config.get('target_database', 'SCHOOL'),
                        "table": table_name,
                        "data": chunk,
                        "is_first_batch": is_first_batch,
                        "is_last_batch": is_last_batch
                    }

                    success = False
                    for retry in range(3):
                        try:
                            response = requests.post(
                                sync_endpoint,
                                data=json.dumps(payload, cls=DecimalEncoder),
                                headers=headers,
                                timeout=180
                            )

                            if response.status_code == 200:
                                response_data = response.json()
                                if response_data.get('success', False):
                                    success = True
                                    break
                                else:
                                    print(
                                        f"\n   ⚠️  API Error: {response_data.get('error', 'Unknown error')}")
                            else:
                                print(
                                    f"\n   ⚠️  Retry {retry + 1}/3 (Status: {response.status_code})")
                                time.sleep(2)

                        except Exception as e:
                            print(
                                f"\n   ⚠️  Retry {retry + 1}/3 (Error: {str(e)})")
                            time.sleep(2)

                    print_progress_bar(chunk_index, len(
                        chunks), f"   Batch {chunk_index}/{len(chunks)}")

                    if not success:
                        print(
                            f"\n❌ Failed to sync {table_name} after 3 attempts")
                        return False

                print(
                    f"   ✅ {table_name} synced successfully! (Total: {len(table_data):,} records)")
                print()

        return True

    except Exception as e:
        print(f"❌ Sync Error: {str(e)}")
        return False


def main():
    """Main function to run the optimized sync process"""
    try:
        print_header()

        # Load configuration
        config = load_config()

        # Connect to database
        conn = connect_to_database(config)

        # Fetch data with optimization
        data = fetch_data_parallel(conn)

        # Close database connection early to free resources
        conn.close()
        print("🔌 Database connection closed")
        print()

        # Try optimized bulk sync first, fallback to legacy if needed
        print("🚀 Attempting optimized bulk sync...")
        success = sync_data_bulk_optimized(data, config)

        if not success:
            print("\n⚠️  Bulk sync failed, trying legacy batch mode...")
            success = sync_data_to_api_legacy(data, config)

        if success:
            print("=" * 70)
            print("           🎉 SYNC COMPLETED SUCCESSFULLY! 🎉")
            print("=" * 70)
            print("✅ All school data has been synchronized to the web database")
            print(
                f"✅ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 70)
            print()
            print("This window will close automatically in 5 seconds...")

            for i in range(5, 0, -1):
                print(f"Closing in {i}...", end="\r", flush=True)
                time.sleep(1)
            sys.exit(0)
        else:
            print("=" * 70)
            print("            ❌ SYNC FAILED! ❌")
            print("=" * 70)
            print("Please check the errors above and try again.")
            print("Common solutions:")
            print("• Check internet connection")
            print("• Verify API server is running")
            print("• Check configuration settings")
            print("• Verify database credentials are correct")
            print("• Try reducing batch size if timeout errors occur")
            print("=" * 70)
            print()
            input("Press Enter to close...")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n⚠️  Sync cancelled by user")
        input("Press Enter to close...")
        sys.exit(1)
    except Exception as e:
        print("\n" + "=" * 70)
        print("            💥 UNEXPECTED ERROR! 💥")
        print("=" * 70)
        print(f"Error: {str(e)}")
        print("\nPlease contact technical support with this error message.")
        print("=" * 70)
        input("\nPress Enter to close...")
        sys.exit(1)


if __name__ == "__main__":
    main()
