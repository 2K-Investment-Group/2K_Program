import os
import pandas as pd
from fredapi import Fred
from datetime import datetime, timedelta
import logging
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from Data.config import config_loader  
from utils.logger_config import setup_logging

logger = logging.getLogger(__name__)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

RAW_DATA_ROOT = os.path.join(BASE_DIR, "data", "raw_data")

FRED_DATA_FOLDER = os.path.join(RAW_DATA_ROOT, "fred")


def ensure_fred_data_folder_exists():
    if not os.path.exists(FRED_DATA_FOLDER):
        os.makedirs(FRED_DATA_FOLDER)
        logger.info(f"Created FRED data storage folder: '{FRED_DATA_FOLDER}'.")

def get_fred_api_key_from_config():
    api_key = config_loader.CONFIG.get('api_keys', {}).get('fred')
    if not api_key:
        logger.critical("Failed to retrieve FRED API key. Please check the 'api_keys: fred: YOUR_FRED_API_KEY' setting in 'config.yaml'.")
        return None
    return api_key

def collect_fred_series(series_id, start_date_str=None, end_date_str=None):
    api_key = get_fred_api_key_from_config()
    if not api_key:
        return False

    fred = Fred(api_key=api_key)

    logger.info(f"Starting download of '{series_id}' data...")
    
    cleaned_series_id = series_id.replace('/', '_').replace('.', '_')
    file_name = f"{cleaned_series_id}.csv"
    full_save_path = os.path.join(FRED_DATA_FOLDER, file_name)

    ensure_fred_data_folder_exists()
    logger.info(f"Data will be saved to '{full_save_path}'.")

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
        data_df.index.name = 'Date'
        data_df.columns = ['Value']

        data_df.to_csv(full_save_path)
        logger.info(f"'{series_id}' data successfully saved to '{full_save_path}'.")
        return True

    except Exception as e:
        logger.error(f"Unexpected error occurred during '{series_id}' data download or save: {e}", exc_info=True)
        logger.error("Please check if the FRED API key is valid, network connectivity, and if the FRED series ID is correct.")
        return False

if __name__ == "__main__":
    setup_logging()

    logger.info("Running FRED_collector.py script directly (for testing purposes).")

    test_fred_series_ids = config_loader.CONFIG['data_sources'].get('fred_series', [])

    if not test_fred_series_ids:
        logger.warning("The 'fred_series' list under 'data_sources' in config.yaml is empty. Skipping FRED test run.")
        sys.exit(0)

    ensure_fred_data_folder_exists()

    total_datasets = len(test_fred_series_ids)
    succeeded_count = 0

    for i, series_id in enumerate(test_fred_series_ids):
        logger.info(f"\n--- [{i+1}/{total_datasets}] Attempting to download '{series_id}' ---")
        if collect_fred_series(series_id): 
            succeeded_count += 1
        else:
            logger.error(f"--- Failed to download '{series_id}' ---")

    logger.info(f"\n--- FRED_collector.py test run completed. {succeeded_count} out of {total_datasets} succeeded. ---")