import os
import pandas as pd
from fredapi import Fred
from datetime import datetime, timedelta
import logging
import sys
import yaml
from sqlalchemy.types import Date, Float, String 
from sqlalchemy import text 

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from Data_Collection.config import config_loader  
from utils.logger_config import setup_logging
from Data_Collection.storage.db_utils import get_db_engine

logger = logging.getLogger(__name__)

def get_fred_api_key_from_config():
    api_key = config_loader.CONFIG.get('api_keys', {}).get('fred') 

    if not api_key:
        logger.critical("Failed to retrieve FRED API key. Please check the 'api_keys: fred' setting in 'config.yaml' and ensure the 'FRED_API_KEY' environment variable is correctly set in your .env file.")
        return None
    return api_key


def collect_fred_series(series_id, series_name, start_date_str=None, end_date_str=None):
    """
    Downloads a single FRED series data and saves it to the database.
    :param series_id: FRED series ID (e.g., 'GDP', 'UNRATE')
    :param series_name: Display name for logging (e.g., '미국 CPI: 에너지')
    :param start_date_str: Start date string (YYYY-MM-DD), uses FRED default if None
    :param end_date_str: End date string (YYYY-MM-DD), uses today's date if None or 'latest'
    """
    api_key = get_fred_api_key_from_config()
    if not api_key:
        return False

    fred = Fred(api_key=api_key)

    logger.info(f"Starting download of '{series_name}' (ID: {series_id}) data...")
    
    # DB Engine
    engine = get_db_engine()
    if not engine:
        logger.error(f"Failed to get DB engine for '{series_name}' (ID: {series_id}). Cannot save to database.")
        return False

    start_date = None
    if start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        except ValueError:
            logger.error(f"'{series_name}' (ID: {series_id}'): Invalid start date format: '{start_date_str}'. Please use YYYY-MM-DD. Attempting full data download.")
            start_date = None

    end_date = datetime.now()
    if end_date_str:
        if end_date_str.lower() == 'latest':
            end_date = datetime.now()
        else:
            try:
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            except ValueError:
                logger.error(f"'{series_name}' (ID: {series_id}'): Invalid end date format: '{end_date_str}'. Please use YYYY-MM-DD. Downloading data up to today.")
                end_date = datetime.now()

    try:
        data = fred.get_series(series_id, observation_start=start_date, observation_end=end_date)
        
        if data is None or data.empty:
            logger.warning(f"'{series_name}' (ID: {series_id}) data is empty. No data fetched from FRED. Check the series ID or if data exists for the specified period.")
            return False
        
        data.name = 'value' 
        data_df = data.reset_index()
        data_df.columns = ['date', 'value']

        data_df['date'] = pd.to_datetime(data_df['date'])
        data_df['value'] = pd.to_numeric(data_df['value'], errors='coerce')
        data_df['series_id'] = series_id 

        table_name = "fred_series_raw"
        try:
            dtype_mapping = {
                'date': Date,
                'value': Float,
                'series_id': String(50) 
            }
            
            data_df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_mapping)
            logger.info(f"'{series_name}' (ID: {series_id}) data successfully saved to database table '{table_name}'. Total {len(data_df)} records.")
            return True
        except Exception as e:
            if "duplicate key value violates unique constraint" in str(e) or "23505" in str(getattr(e, 'orig', '')):
                logger.warning(f"'{series_id}' data for some dates already exists in '{table_name}'. New data appended, existing dates skipped/not updated. Error: {e}")
                return True
            else:
                logger.error(f"Error saving '{series_name}' (ID: {series_id}) data to database: {e}", exc_info=True)
                return False

    except ValueError as e:
        logger.error(f"FRED API Error for '{series_name}' (ID: {series_id}): {e}")
        logger.error("Please check if the FRED API key is valid, network connectivity, and if the FRED series ID is correct.")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred during '{series_name}' (ID: {series_id}) data download or save: {e}", exc_info=True)
        return False
    finally:
        if engine:
            engine.dispose()

if __name__ == "__main__":
    setup_logging()

    logger.info("Running FRED_collector.py script directly (for testing purposes).")

    test_fred_datasets_list = config_loader.CONFIG.get('fred_datasets', []) 

    if not test_fred_datasets_list:
        logger.warning("The 'fred_datasets' list in config.yaml is empty or missing. Skipping FRED data collection.")
        sys.exit(0)

    total_datasets = len(test_fred_datasets_list)
    succeeded_count = 0

    for i, dataset_info in enumerate(test_fred_datasets_list):
        series_id = dataset_info.get('series_id')
        start_date_str = dataset_info.get('start_date')
        end_date_str = dataset_info.get('end_date')
        
        display_name = dataset_info.get('name', series_id) 

        if not series_id:
            logger.error(f"--- Skipped dataset {i+1}: Missing 'series_id' in config entry: {dataset_info} ---")
            continue

        logger.info(f"\n--- [{i+1}/{total_datasets}] Attempting to download '{display_name}' (ID: {series_id}) ---")
        
        if collect_fred_series(series_id, display_name, start_date_str, end_date_str): 
            succeeded_count += 1
        else:
            logger.error(f"--- Failed to download '{display_name}' (ID: {series_id}) ---")

    logger.info(f"\n--- FRED_collector.py test run completed. {succeeded_count} out of {total_datasets} succeeded. ---")