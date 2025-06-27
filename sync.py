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


# Clear old logs and setup new logging
def setup_logging():
    """Clear old logs and setup fresh logging"""
    log_file = 'sync.log'
    
    # Remove old log file if exists
    if os.path.exists(log_file):
        try:
            os.remove(log_file)
            print("🗑️  Cleared old sync log")
        except:
            pass
    
    # Setup logging with better formatting
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True  # Force reconfiguration
    )
    return logging.getLogger(__name__)


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
    """Load minimal configuration from config.json file"""
    CONFIG_FILE = 'config.json'
    try:
        print("📋 Loading configuration file...")
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        
        # Validate required fields
        if 'dsn' not in config:
            raise ValueError("Missing 'dsn' in config.json")
        if 'api_url' not in config:
            raise ValueError("Missing 'api_url' in config.json")
            
        print("✅ Configuration loaded successfully")
        print(f"   → DSN: {config['dsn']}")
        print(f"   → API URL: {config['api_url']}")
        print()
        return config
    except FileNotFoundError:
        logger.info(f"❌ ERROR: Configuration file '{CONFIG_FILE}' not found!")
        logger.info("📋 Expected format:")
        logger.info('   {')
        logger.info('     "dsn": "your_dsn_name",')
        logger.info('     "api_url": "https://your-api-url.com"')
        logger.info('   }')
        input("\nPress Enter to exit...")
        sys.exit(1)
    except (json.JSONDecodeError, ValueError) as e:
        logger.info(f"❌ ERROR: Configuration error: {e}")
        logger.info("📋 Expected format:")
        logger.info('   {')
        logger.info('     "dsn": "your_dsn_name",')
        logger.info('     "api_url": "https://your-api-url.com"')
        print('   }')
        input("\nPress Enter to exit...")
        sys.exit(1)


def connect_to_database(config):
    """Connect to SQL Anywhere database using ODBC with hardcoded credentials"""
    try:
        print("🔌 Connecting to database...")
        
        # Hardcoded database credentials
        dsn = config['dsn']
        username = "DBA"
        password = "(*$^)"
        
        print(f"   → DSN: {dsn}")
        print(f"   → User: {username}")

        conn_str = f"DSN={dsn};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str)
        print("✅ Database connection successful!\n")
        
        logger.info(f"Database connection established - DSN: {dsn}, User: {username}")
        return conn
    except pyodbc.Error as e:
        error_msg = f"Database connection failed: {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)
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
        logger.info(f"Starting to fetch {total_count} records from {table_name}")

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
        logger.info(f"Successfully fetched {len(results)} records from {table_name}")
        return results

    except pyodbc.Error as e:
        error_msg = f"Query execution failed for {table_name}: {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)
        return []


def fetch_data_parallel(conn):
    """Fetch data from all required tables using parallel processing"""
    print("📊 FETCHING DATA FROM DATABASE (OPTIMIZED)")
    print("-" * 50)
    logger.info("Starting data fetch from database")

    # Hardcoded queries for required tables
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
        logger.info(f"Table {table_name}: {record_count} records loaded")

    print("-" * 50)
    print(f"📈 TOTAL RECORDS TO SYNC: {total_records:,}")
    logger.info(f"Total records to sync: {total_records}")
    print()

    return data


def reset_sync_session(config):
    """Reset the sync session on the API server"""
    try:
        api_base_url = config['api_url']
        reset_endpoint = f"{api_base_url}/api/reset-sync-session/"

        print("🔄 Resetting sync session on server...")
        logger.info(f"Sending reset request to: {reset_endpoint}")

        response = requests.post(reset_endpoint,
                                 headers={'Content-Type': 'application/json'},
                                 timeout=30)

        if response.status_code == 200:
            print("✅ Sync session reset on server")
            logger.info("Sync session reset successful")
            return True
        else:
            warning_msg = f"Could not reset sync session on server (Status: {response.status_code})"
            print(f"⚠️  Warning: {warning_msg}")
            logger.warning(warning_msg)
            return False
    except Exception as e:
        warning_msg = f"Could not reset sync session: {e}"
        print(f"⚠️  Warning: {warning_msg}")
        logger.warning(warning_msg)
        return False


def sync_data_bulk_optimized(data, config):
    """Optimized bulk sync - send all data in one request"""
    try:
        api_base_url = config['api_url']
        print(f"🌐 API Server: {api_base_url}")
        logger.info(f"Starting bulk sync to API server: {api_base_url}")
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
        logger.info("Preparing bulk sync payload")

        # Calculate total records
        total_records = sum(len(table_data) for table_data in data.values())
        print(f"📊 Total records to sync: {total_records:,}")

        # Hardcoded target database and prepare payload
        target_database = "safa"  # Hardcoded
        payload = {
            "database": target_database,
            "tables": data,
            "total_records": total_records,
            "sync_timestamp": datetime.now().isoformat()
        }

        print("🔄 Serializing data to JSON...")
        logger.info("Serializing data to JSON format")
        json_data = json.dumps(payload, cls=DecimalEncoder)
        data_size_mb = len(json_data.encode('utf-8')) / (1024 * 1024)
        print(f"📦 Payload size: {data_size_mb:.2f} MB")
        logger.info(f"Payload size: {data_size_mb:.2f} MB")

        print("\n📤 SENDING BULK SYNC REQUEST")
        print("-" * 50)
        print("⏳ Uploading data to server...")
        logger.info(f"Sending POST request to: {bulk_sync_endpoint}")

        # Dynamic timeout based on data size
        timeout = max(300, total_records // 1000 * 10)
        logger.info(f"Request timeout set to: {timeout} seconds")

        start_time = time.time()

        try:
            print("🌐 Establishing connection to server...")
            logger.info("Making HTTP POST request to API server")
            
            response = requests.post(
                bulk_sync_endpoint,
                data=json_data,
                headers=headers,
                timeout=timeout
            )

            upload_time = time.time() - start_time
            print(f"⏱️  Upload completed in {upload_time:.2f} seconds")
            logger.info(f"Server response received in {upload_time:.2f} seconds")
            logger.info(f"Response status code: {response.status_code}")

            if response.status_code == 200:
                print("📥 Processing server response...")
                logger.info("Processing successful server response")
                
                response_data = response.json()
                logger.info(f"Server response data: {response_data}")
                
                if response_data.get('success', False):
                    print("✅ BULK SYNC SUCCESSFUL!")
                    success_msg = f"Total records processed: {response_data.get('total_records', 0)}, Tables processed: {response_data.get('tables_processed', 0)}"
                    print(f"📊 {success_msg}")
                    logger.info(f"Bulk sync successful - {success_msg}")

                    # Show detailed results
                    results = response_data.get('results', {})
                    print("\n📈 DETAILED RESULTS:")
                    print("-" * 30)
                    for table_name, table_result in results.items():
                        records = table_result.get('records_processed', 0)
                        print(f"  {table_name}: {records:,} records")
                        logger.info(f"Table {table_name}: {records} records processed")

                    return True
                else:
                    error_msg = response_data.get('error', 'Unknown error')
                    print(f"❌ API Error: {error_msg}")
                    logger.error(f"API returned error: {error_msg}")
                    
                    if 'validation_errors' in response_data:
                        print("📋 Validation errors:")
                        validation_errors = response_data['validation_errors'][:5]
                        logger.error(f"Validation errors: {validation_errors}")
                        for error in validation_errors:
                            print(f"  Row {error.get('row', '?')}: {error.get('errors', {})}")
                    return False
            else:
                error_msg = f"HTTP Error: {response.status_code}"
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                
                try:
                    error_data = response.json()
                    print(f"📋 Error details: {error_data}")
                    logger.error(f"Error response: {error_data}")
                except:
                    response_text = response.text[:500]
                    print(f"📋 Response text: {response_text}...")
                    logger.error(f"Error response text: {response_text}")
                return False

        except requests.exceptions.Timeout:
            timeout_msg = f"Request timed out after {timeout} seconds"
            print(f"⏰ {timeout_msg}")
            print("💡 Suggestion: Try increasing timeout or reducing batch size")
            logger.error(timeout_msg)
            return False
        except requests.exceptions.RequestException as e:
            network_error = f"Network error: {str(e)}"
            print(f"🌐 {network_error}")
            logger.error(network_error)
            return False

    except Exception as e:
        sync_error = f"Sync Error: {str(e)}"
        print(f"❌ {sync_error}")
        logger.error(sync_error)
        return False


def sync_data_to_api_legacy(data, config):
    """Legacy batch sync method - kept as fallback"""
    try:
        api_base_url = config['api_url']
        print(f"🌐 API Server: {api_base_url} (Legacy Mode)")
        logger.info(f"Starting legacy sync mode to: {api_base_url}")
        print()

        reset_sync_session(config)

        headers = {'Content-Type': 'application/json'}
        sync_endpoint = f"{api_base_url}/api/sync/"

        # Hardcoded batch size and target database
        BATCH_SIZE = 3000
        target_database = "safa"

        tables_to_sync = ["acc_users", "personel",
                          "mag_subject", "cce_assessmentitems", "cce_entry"]

        print("📤 SYNCING DATA TO API (LEGACY MODE)")
        print("-" * 50)
        logger.info("Starting legacy batch sync")

        def chunk_data(data_list, chunk_size=BATCH_SIZE):
            for i in range(0, len(data_list), chunk_size):
                yield data_list[i:i + chunk_size]

        for table_index, table_name in enumerate(tables_to_sync, 1):
            if table_name in data:
                table_data = data[table_name]
                if not table_data:
                    print(f"{table_index}. {table_name}: No data to sync")
                    logger.info(f"Table {table_name}: No data to sync")
                    continue

                print(f"{table_index}. Syncing {len(table_data):,} records from {table_name}...")
                logger.info(f"Starting sync for table {table_name} with {len(table_data)} records")

                chunks = list(chunk_data(table_data, chunk_size=BATCH_SIZE))

                for chunk_index, chunk in enumerate(chunks, 1):
                    print_progress_bar(
                        chunk_index - 1, len(chunks), f"   Batch {chunk_index}/{len(chunks)}")

                    is_first_batch = (chunk_index == 1)
                    is_last_batch = (chunk_index == len(chunks))

                    payload = {
                        "database": target_database,
                        "table": table_name,
                        "data": chunk,
                        "is_first_batch": is_first_batch,
                        "is_last_batch": is_last_batch
                    }

                    logger.info(f"Sending batch {chunk_index}/{len(chunks)} for table {table_name}")

                    success = False
                    for retry in range(3):
                        try:
                            logger.info(f"Attempt {retry + 1}/3 for batch {chunk_index}")
                            response = requests.post(
                                sync_endpoint,
                                data=json.dumps(payload, cls=DecimalEncoder),
                                headers=headers,
                                timeout=180
                            )

                            logger.info(f"Response status: {response.status_code}")

                            if response.status_code == 200:
                                response_data = response.json()
                                if response_data.get('success', False):
                                    success = True
                                    logger.info(f"Batch {chunk_index} sent successfully")
                                    break
                                else:
                                    error_msg = response_data.get('error', 'Unknown error')
                                    print(f"\n   ⚠️  API Error: {error_msg}")
                                    logger.error(f"API error for batch {chunk_index}: {error_msg}")
                            else:
                                print(f"\n   ⚠️  Retry {retry + 1}/3 (Status: {response.status_code})")
                                logger.warning(f"Retry {retry + 1}/3 - Status: {response.status_code}")
                                time.sleep(2)

                        except Exception as e:
                            print(f"\n   ⚠️  Retry {retry + 1}/3 (Error: {str(e)})")
                            logger.error(f"Retry {retry + 1}/3 - Error: {str(e)}")
                            time.sleep(2)

                    print_progress_bar(chunk_index, len(
                        chunks), f"   Batch {chunk_index}/{len(chunks)}")

                    if not success:
                        error_msg = f"Failed to sync {table_name} after 3 attempts"
                        print(f"\n❌ {error_msg}")
                        logger.error(error_msg)
                        return False

                success_msg = f"{table_name} synced successfully! (Total: {len(table_data)} records)"
                print(f"   ✅ {success_msg}")
                logger.info(success_msg)
                print()

        return True

    except Exception as e:
        sync_error = f"Legacy sync error: {str(e)}"
        print(f"❌ {sync_error}")
        logger.error(sync_error)
        return False


def main():
    """Main function to run the optimized sync process"""
    global logger
    
    try:
        print_header()

        # Setup fresh logging
        logger = setup_logging()
        logger.info("=== SYNC SESSION STARTED ===")

        # Load minimal configuration
        config = load_config()

        # Connect to database
        conn = connect_to_database(config)

        # Fetch data with optimization
        data = fetch_data_parallel(conn)

        # Close database connection early to free resources
        conn.close()
        print("🔌 Database connection closed")
        logger.info("Database connection closed")
        print()

        # Try optimized bulk sync first, fallback to legacy if needed
        print("🚀 Attempting optimized bulk sync...")
        logger.info("Attempting optimized bulk sync")
        success = sync_data_bulk_optimized(data, config)

        if not success:
            print("\n⚠️  Bulk sync failed, trying legacy batch mode...")
            logger.warning("Bulk sync failed, attempting legacy mode")
            success = sync_data_to_api_legacy(data, config)

        if success:
            print("=" * 70)
            print("           🎉 SYNC COMPLETED SUCCESSFULLY! 🎉")
            print("=" * 70)
            success_msg = "All school data has been synchronized to the web database"
            print(f"✅ {success_msg}")
            print(f"✅ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 70)
            logger.info("=== SYNC COMPLETED SUCCESSFULLY ===")
            logger.info(success_msg)
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
            error_msg = "Sync process failed - check logs for details"
            print("Please check the errors above and try again.")
            print("Common solutions:")
            print("• Check internet connection")
            print("• Verify API server is running")
            print("• Check configuration settings")
            print("• Verify database credentials are correct")
            print("• Try reducing batch size if timeout errors occur")
            print("=" * 70)
            logger.error("=== SYNC FAILED ===")
            logger.error(error_msg)
            print()
            input("Press Enter to close...")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n⚠️  Sync cancelled by user")
        logger.warning("Sync cancelled by user")
        input("Press Enter to close...")
        sys.exit(1)
    except Exception as e:
        print("\n" + "=" * 70)
        print("            💥 UNEXPECTED ERROR! 💥")
        print("=" * 70)
        error_msg = f"Unexpected error: {str(e)}"
        print(f"Error: {str(e)}")
        print("\nPlease contact technical support with this error message.")
        print("=" * 70)
        logger.error("=== UNEXPECTED ERROR ===")
        logger.error(error_msg)
        input("\nPress Enter to close...")
        sys.exit(1)


if __name__ == "__main__":
    main()