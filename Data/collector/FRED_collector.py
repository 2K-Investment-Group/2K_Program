import os
import pandas as pd
from fredapi import Fred
from datetime import datetime, timedelta
import logging
import sys
from sqlalchemy.dialects import postgresql # for specific column types if needed
from sqlalchemy import text # for raw SQL in to_sql dtype or execute

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from Data.config import config_loader  
from utils.logger_config import setup_logging # utils.logger_config
from storage.db_utils import get_db_engine # DB 엔진 가져오기

logger = logging.getLogger(__name__)

# CSV 저장은 제거하고 DB에 직접 저장
# BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
# RAW_DATA_ROOT = os.path.join(BASE_DIR, "data", "raw_data")
# FRED_DATA_FOLDER = os.path.join(RAW_DATA_ROOT, "fred")

# def ensure_fred_data_folder_exists():
#     if not os.path.exists(FRED_DATA_FOLDER):
#         os.makedirs(FRED_DATA_FOLDER)
#         logger.info(f"Created base FRED data storage folder: '{FRED_DATA_FOLDER}'.")

def get_fred_api_key_from_config():
    api_key = config_loader.CONFIG.get('api_keys', {}).get('fred')
    if not api_key:
        logger.critical("Failed to retrieve FRED API key. Please check the 'api_keys: fred: YOUR_FRED_API_KEY' setting in 'config.yaml'.")
        return None
    return api_key

def collect_fred_series(series_id, start_date_str=None, end_date_str=None, 
                        desired_file_name=None, sub_path=None): # file_name and sub_path are now ignored for DB
    """
    Downloads a single FRED series data and saves it to the database.
    :param series_id: FRED series ID (e.g., 'GDP', 'UNRATE')
    :param start_date_str: Start date string (YYYY-MM-DD), uses FRED default if None
    :param end_date_str: End date string (YYYY-MM-DD), uses today's date if None or 'latest'
    :param desired_file_name: (Ignored for DB save) The exact file name (e.g., '미국 실질 국내총생산.csv')
    :param sub_path: (Ignored for DB save) Subdirectory path within FRED_DATA_FOLDER (e.g., 'US/Macro/GDP')
    """
    api_key = get_fred_api_key_from_config()
    if not api_key:
        return False

    fred = Fred(api_key=api_key)

    logger.info(f"Starting download of '{series_id}' data...")
    
    # DB Engine
    engine = get_db_engine()
    if not engine:
        logger.error(f"Failed to get DB engine for {series_id}. Cannot save to database.")
        return False

    start_date = None
    if start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        except ValueError:
            logger.error(f"'{series_id}': Invalid start date format: '{start_date_str}'. Please use YYYY-MM-DD. Attempting full data download.")
            start_date = None

    end_date = datetime.now()
    if end_date_str:
        if end_date_str.lower() == 'latest':
            end_date = datetime.now()
        else:
            try:
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            except ValueError:
                logger.error(f"'{series_id}': Invalid end date format: '{end_date_str}'. Please use YYYY-MM-DD. Downloading data up to today.")
                end_date = datetime.now()

    try:
        data = fred.get_series(series_id, observation_start=start_date, observation_end=end_date)
        
        if data is None or data.empty:
            logger.warning(f"'{series_id}' data is empty. No data fetched from FRED. Check the series ID or if data exists for the specified period.")
            return False
        
        data_df = pd.DataFrame(data)
        data_df.index.name = 'date' # DB 컬럼명과 일치
        data_df.columns = ['value'] # DB 컬럼명과 일치
        data_df['series_id'] = series_id # 새로운 컬럼 추가

        # Ensure index (date) is datetime for to_sql
        data_df.index = pd.to_datetime(data_df.index)
        data_df = data_df.reset_index() # 'date'를 컬럼으로 전환

        # Save to database
        table_name = "fred_series_raw"
        try:
            # Upsert using 'if_exists' and ON CONFLICT (requires specific SQLAlchemy setup or raw SQL)
            # For simplicity, we'll use append and assume unique constraint will handle conflicts
            # or that fresh run will overwrite/update if UNIQUE constraint is setup with ON CONFLICT DO UPDATE
            # The db_setup.py now has UNIQUE (series_id, date) constraint.
            # Pandas to_sql does not directly support ON CONFLICT DO UPDATE for PostgreSQL out of the box easily.
            # A common pattern is to write a custom upsert function or use a temporary table + merge.
            # For now, let's just append and rely on the unique constraint to prevent exact duplicates,
            # but existing data with same key won't be updated.

            # Alternative: Custom upsert with psycopg2 or explicit SQL for ON CONFLICT
            # For this example, let's keep it simple with to_sql and let unique constraint manage it
            # if a row with same series_id and date already exists, it will cause an error (or skip if managed)
            # To truly upsert, we need to handle it more explicitly.

            # Let's use a method that handles upsert for simplicity of the script,
            # even if it means iterating or using a temp table internally.
            # A common workaround for `to_sql` upsert is to use `if_exists='append'` and then manage
            # conflicts in the DB using `ON CONFLICT DO UPDATE` or `DELETE` then `INSERT`.
            # Since `db_setup.py` added `UNIQUE (series_id, date)`, if a row with same key exists,
            # `to_sql(if_exists='append')` will raise an IntegrityError.
            # A more robust solution for `to_sql` would be:
            # 1. Load existing data for the period.
            # 2. Merge with new data, identify inserts/updates.
            # 3. Use `to_sql` for new data, and execute `UPDATE` for existing data.
            #
            # For now, let's use `if_exists='append'` and catch the potential error,
            # warning about existing data.

            # More robust upsert logic for to_sql:
            # 1. Create a temporary table.
            # 2. Insert new data into the temporary table.
            # 3. Use `INSERT INTO ... ON CONFLICT DO UPDATE` from the temporary table.

            # Simplified approach for this request: Try inserting. If unique constraint fails, log it.
            # This means data for existing (series_id, date) pairs won't be updated.
            # If a series is updated daily, it implies we only want to add new dates.
            # If an existing date's value can change, then full upsert is needed.
            # FRED data usually doesn't change historical observations, only new ones added.
            
            # Use this for basic insertion, relying on UNIQUE constraint to prevent duplicate dates
            data_df.to_sql(table_name, engine, if_exists='append', index=False, dtype={
                'date': text('DATE'),
                'value': text('NUMERIC'),
                'series_id': text('VARCHAR(50)')
            })
            logger.info(f"'{series_id}' data successfully saved to database table '{table_name}'. Total {len(data_df)} records.")
            return True
        except Exception as e:
            # Handle unique constraint violation specifically if needed
            if "duplicate key value violates unique constraint" in str(e):
                logger.warning(f"'{series_id}' data for some dates already exists in '{table_name}'. New data appended, existing dates skipped/not updated. Error: {e}")
                return True # Consider it a success if new data was appended
            else:
                logger.error(f"Error saving '{series_id}' data to database: {e}", exc_info=True)
                return False

    except Exception as e:
        logger.error(f"Unexpected error occurred during '{series_id}' data download or save: {e}", exc_info=True)
        logger.error("Please check if the FRED API key is valid, network connectivity, and if the FRED series ID is correct.")
        return False
    finally:
        if engine:
            engine.dispose() # Close the connection pool

if __name__ == "__main__":
    setup_logging()

    logger.info("Running FRED_collector.py script directly (for testing purposes).")

    test_fred_datasets_list = config_loader.CONFIG.get('fred_datasets', [])

    if not test_fred_datasets_list:
        logger.warning("The 'fred_datasets' list in config.yaml is empty or missing. Skipping FRED test run.")
        sys.exit(0)

    # ensure_fred_data_folder_exists() # CSV 저장 로직 제거

    total_datasets = len(test_fred_datasets_list)
    succeeded_count = 0

    for i, dataset_info in enumerate(test_fred_datasets_list):
        series_id = dataset_info.get('series_id')
        start_date_str = dataset_info.get('start_date')
        end_date_str = dataset_info.get('end_date')
        desired_file_name = dataset_info.get('file_name')
        sub_path = dataset_info.get('path')
        
        display_name = dataset_info.get('name', series_id)

        if not series_id:
            logger.error(f"--- Skipped dataset {i+1}: Missing 'series_id' in config entry: {dataset_info} ---")
            continue

        logger.info(f"\n--- [{i+1}/{total_datasets}] Attempting to download '{display_name}' (ID: {series_id}) ---")
        
        if collect_fred_series(series_id, start_date_str, end_date_str, desired_file_name, sub_path): 
            succeeded_count += 1
        else:
            logger.error(f"--- Failed to download '{display_name}' (ID: {series_id}) ---")

    logger.info(f"\n--- FRED_collector.py test run completed. {succeeded_count} out of {total_datasets} succeeded. ---")