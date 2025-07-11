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
    # This function now ensures the base FRED data folder exists.
    # Subfolders will be created dynamically within collect_fred_series.
    if not os.path.exists(FRED_DATA_FOLDER):
        os.makedirs(FRED_DATA_FOLDER)
        logger.info(f"Created base FRED data storage folder: '{FRED_DATA_FOLDER}'.")

def get_fred_api_key_from_config():
    api_key = config_loader.CONFIG.get('api_keys', {}).get('fred')
    if not api_key:
        logger.critical("Failed to retrieve FRED API key. Please check the 'api_keys: fred: YOUR_FRED_API_KEY' setting in 'config.yaml'.")
        return None
    return api_key

def collect_fred_series(series_id, start_date_str=None, end_date_str=None, 
                        desired_file_name=None, sub_path=None):
    """
    Downloads a single FRED series data and saves it to a specified path as a CSV file.
    This function is designed to be called by main.py, taking series_id and optional dates.
    It now also accepts a desired_file_name and a sub_path for organized saving.

    :param series_id: FRED series ID (e.g., 'GDP', 'UNRATE')
    :param start_date_str: Start date string (YYYY-MM-DD), uses FRED default if None
    :param end_date_str: End date string (YYYY-MM-DD), uses today's date if None or 'latest'
    :param desired_file_name: The exact file name (e.g., '미국 실질 국내총생산.csv')
    :param sub_path: Subdirectory path within FRED_DATA_FOLDER (e.g., 'US/Macro/GDP')
    """
    api_key = get_fred_api_key_from_config()
    if not api_key:
        return False

    fred = Fred(api_key=api_key)

    logger.info(f"Starting download of '{series_id}' data...")
    
    # Determine the full save directory, including sub_path
    save_dir = FRED_DATA_FOLDER
    if sub_path:
        save_dir = os.path.join(FRED_DATA_FOLDER, sub_path)
    
    # Ensure the target directory exists (including any subdirectories)
    os.makedirs(save_dir, exist_ok=True) # exist_ok=True prevents error if dir already exists

    # Use the provided desired_file_name, otherwise fall back to a cleaned series_id
    if desired_file_name:
        file_name = desired_file_name
    else:
        cleaned_series_id = series_id.replace('/', '_').replace('.', '_')
        file_name = f"{cleaned_series_id}.csv"

    full_save_path = os.path.join(save_dir, file_name)
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
        data_df.columns = ['Value'] # FRED data typically consists of a single value.

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

    # Get fred_datasets directly from config_loader.CONFIG as a list of dictionaries
    # Provide an empty list as default if 'fred_datasets' key is not found
    test_fred_datasets_list = config_loader.CONFIG.get('fred_datasets', [])

    if not test_fred_datasets_list: # Check if the list is empty
        logger.warning("The 'fred_datasets' list in config.yaml is empty or missing. Skipping FRED test run.")
        sys.exit(0)

    # Ensure the base FRED data folder exists once at the start
    ensure_fred_data_folder_exists() 

    total_datasets = len(test_fred_datasets_list)
    succeeded_count = 0

    # Iterate through each dictionary in the list
    for i, dataset_info in enumerate(test_fred_datasets_list):
        # Extract required parameters from the current dictionary
        series_id = dataset_info.get('series_id')
        start_date_str = dataset_info.get('start_date')
        end_date_str = dataset_info.get('end_date')
        desired_file_name = dataset_info.get('file_name') # Get the specified file_name
        sub_path = dataset_info.get('path') # Get the specified path for subfolders
        
        display_name = dataset_info.get('name', series_id) # Use 'name' for logging

        if not series_id:
            logger.error(f"--- Skipped dataset {i+1}: Missing 'series_id' in config entry: {dataset_info} ---")
            continue # Skip to the next item if series_id is missing

        logger.info(f"\n--- [{i+1}/{total_datasets}] Attempting to download '{display_name}' (ID: {series_id}, Path: '{sub_path}', File: '{desired_file_name}') ---")
        
        # Pass the extracted file_name and sub_path to the collect_fred_series function
        if collect_fred_series(series_id, start_date_str, end_date_str, desired_file_name, sub_path): 
            succeeded_count += 1
        else:
            logger.error(f"--- Failed to download '{display_name}' (ID: {series_id}) ---")

    logger.info(f"\n--- FRED_collector.py test run completed. {succeeded_count} out of {total_datasets} succeeded. ---")