import requests
import json
import pandas as pd
import os
import re
import logging
from datetime import datetime
import time

from Data.config import config_loader 

from utils.logger_config import setup_logging

logger = logging.getLogger(__name__)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

RAW_DATA_ROOT = os.path.join(BASE_DIR, "data", "raw_data")

WB_DATA_FOLDER = os.path.join(RAW_DATA_ROOT, "world_bank")

WB_API_BASE_URL = "https://api.worldbank.org/v2"
DEFAULT_START_YEAR = 1960 
DEFAULT_END_YEAR = datetime.now().year

DEFAULT_TOP_INDICATORS = {
    "NY.GDP.MKTP.CD": "GDP (current US$)",
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    "NY.GDP.GROW.ZS": "GDP growth (annual %)",
    "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
    "SL.UEM.TOTL.ZS": "Unemployment, total (% of total labor force)",
    "BN.CAB.XOKA.CD": "Current account balance (BoP, current US$)",
    "NV.AGR.TOTL.ZS": "Agriculture, forestry, and fishing, value added (% of GDP)",
    "NV.IND.TOTL.ZS": "Industry (including construction), value added (% of GDP)",
    "NV.SRV.TOTL.ZS": "Services, value added (% of GDP)",
    "BX.GSR.MRCH.CD": "Merchandise exports (current US$)",
    "BM.GSR.MRCH.CD": "Merchandise imports (current US$)",
    "GC.TAX.TOTL.GD.ZS": "Tax revenue (% of GDP)",
    "GC.XPN.TOTL.GD.ZS": "Expense (% of GDP)",
    "FX.GDP.PCAP.CD": "GDP per capita, PPP (current international $)",
    "SP.POP.TOTL": "Population, total",
    "SP.URB.TOTL.IN.ZS": "Urban population (% of total population)",
    "SL.TLF.TOTL.IN": "Labor force, total",
    "SE.XPD.TOTL.GD.ZS": "Expenditure on education (% of GDP)",
    "SH.XPD.CHEX.GD.ZS": "Current health expenditure (% of GDP)",
    "VC.BTL.DETH": "Battle-related deaths (number of people)",
}

def ensure_wb_data_folder_exists():
    if not os.path.exists(WB_DATA_FOLDER):
        os.makedirs(WB_DATA_FOLDER)
        logger.info(f"Created World Bank data storage folder: '{WB_DATA_FOLDER}'.")

def get_api_response(url, logger, retries=3, delay=1): 
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            logger.warning(f"API request error (Attempt {attempt+1}/{retries}): {e} - URL: {url}")
            if attempt < retries - 1:
                time.sleep(delay)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error: {e} - Response text: {response.text[:200]}")
            return None
    logger.error(f"All retries exhausted for API request: {url}")
    return None

def clean_filename(name):
    name = re.sub(r'[^\w\s.-]', '', name)
    name = re.sub(r'\s+', '_', name).strip('_')
    name = name.lower()
    if len(name) > 200:
        name = name[:200]
    return name + ".csv"

def fetch_all_countries(logger):
    url = f"{WB_API_BASE_URL}/country?format=json&per_page=1000"
    data = get_api_response(url, logger)
    if data and len(data) > 1 and data[1] is not None:
        countries = {item['id']: item['name'] for item in data[1]
                     if item['id'] != 'all' and item['region']['id'] != 'NA' and item['incomeLevel']['id'] != 'NA'}
        logger.info(f"Fetched a list of {len(countries)} countries (excluding aggregate groups).")
        return countries
    logger.error("Failed to fetch country list.")
    return {}

def fetch_and_save_indicator_data(country_code, country_name, indicator_code, indicator_name, 
                                  start_year=DEFAULT_START_YEAR, end_year=DEFAULT_END_YEAR):
    all_data = []
    page = 1
    total_pages = 1
    
    base_indicator_url = f"{WB_API_BASE_URL}/country/{country_code}/indicator/{indicator_code}?format=json&date={start_year}:{end_year}&per_page=1000"

    logger.debug(f"  └─ Starting data processing for '{country_name}' ({country_code}) - '{indicator_name}' ({indicator_code})...")

    while page <= total_pages:
        url = f"{base_indicator_url}&page={page}"
        data = get_api_response(url, logger)

        if data and len(data) > 1 and data[1] is not None:
            metadata = data[0]
            current_page_data = data[1]

            for item in current_page_data:
                if item['value'] is not None:
                    all_data.append({
                        'country': item['country']['value'],
                        'country_code': item['countryiso3code'],
                        'indicator': item['indicator']['value'],
                        'indicator_code': item['indicator']['id'],
                        'year': int(item['date']),
                        'value': float(item['value'])
                    })
            total_pages = metadata['pages']
            page += 1
            time.sleep(0.05)
        else:
            if page == 1:
                pass 
            else:
                logger.warning(f"    └─ No more data or an error occurred on page {page} for '{country_name}' - '{indicator_name}'.")
            break

    if all_data:
        df = pd.DataFrame(all_data)
        df = df.sort_values(by=['country_code', 'year']).reset_index(drop=True)

        country_save_dir = os.path.join(WB_DATA_FOLDER, country_name.replace(' ', '_').replace('.', '').replace(',', '')) 
        os.makedirs(country_save_dir, exist_ok=True)

        file_name = clean_filename(indicator_name)
        full_save_path = os.path.join(country_save_dir, file_name)

        try:
            df.to_csv(full_save_path, index=False)
            logger.info(f"    └─ Saved {len(df)} entries for '{indicator_name}' in '{country_name}'.")
            return True
        except Exception as e:
            logger.error(f"    └─ Error saving '{indicator_name}' data for '{country_name}': {e}")
            return False
    else:
        return False

def collect_world_bank_data(countries_to_fetch=None, indicators_to_fetch=None,
                            start_year=None, end_year=None):
    
    current_start_year = start_year if start_year is not None else config_loader.CONFIG.get('data_sources', {}).get('world_bank_start_year', DEFAULT_START_YEAR)
    current_end_year = end_year if end_year is not None else config_loader.CONFIG.get('data_sources', {}).get('world_bank_end_year', DEFAULT_END_YEAR)
    
    current_indicators = indicators_to_fetch if indicators_to_fetch is not None else \
                         config_loader.CONFIG.get('data_sources', {}).get('world_bank_indicators', DEFAULT_TOP_INDICATORS)

    if not current_indicators:
        logger.warning("World Bank indicator list to collect is empty. Skipping collection.")
        return False

    logger.info("Starting World Bank data collection.")
    logger.info(f"Collection period: From {current_start_year} to {current_end_year}")
    logger.info(f"Collecting {len(current_indicators)} indicators.")

    if countries_to_fetch is None:
        countries = fetch_all_countries(logger)
    else:
        all_available_countries = fetch_all_countries(logger)
        countries = {code: name for code, name in all_available_countries.items() if code in countries_to_fetch}
        if not countries:
            logger.warning(f"No countries found in World Bank API among the specified country codes {countries_to_fetch} in config.yaml. Skipping collection.")
            return False

    if not countries:
        logger.critical("Failed to fetch country list. Terminating World Bank data collection.")
        return False

    total_countries = len(countries)
    total_indicators_to_fetch = len(current_indicators)
    logger.info(f"Processing {total_indicators_to_fetch} indicators for {total_countries} countries.")

    ensure_wb_data_folder_exists()

    country_processed_count = 0
    succeeded_data_count = 0

    for country_code, country_name in countries.items():
        country_processed_count += 1
        logger.info(f"\n--- Country {country_processed_count}/{total_countries}: Starting data collection for '{country_name}' ({country_code}) ---")

        indicator_in_country_count = 0
        for indicator_code, indicator_name in current_indicators.items():
            indicator_in_country_count += 1
            logger.info(f"  [ {indicator_in_country_count}/{total_indicators_to_fetch} ] Attempting data for '{indicator_name}'...")
            
            if fetch_and_save_indicator_data(country_code, country_name, indicator_code, indicator_name,
                                             start_year=current_start_year, end_year=current_end_year):
                succeeded_data_count += 1
            
        COUNTRY_PROCESSING_DELAY_SECONDS = config_loader.CONFIG.get('api_delays', {}).get('world_bank_country_delay_seconds', 5)
        logger.info(f"Finished processing data for country '{country_name}' ({country_code}). Waiting {COUNTRY_PROCESSING_DELAY_SECONDS} seconds until next country...")
        time.sleep(COUNTRY_PROCESSING_DELAY_SECONDS)

    logger.info(f"\nAll World Bank data collection attempts completed. Total {succeeded_data_count} indicator-country datasets saved.")
    return True

if __name__ == "__main__":
    setup_logging()

    logger.info("Running WB_collector.py script directly (for testing purposes).")

    test_countries = config_loader.CONFIG['data_sources'].get('world_bank_countries', ['KOR', 'USA'])
    test_indicators = config_loader.CONFIG['data_sources'].get('world_bank_indicators', DEFAULT_TOP_INDICATORS)
    test_start_year = config_loader.CONFIG['data_sources'].get('world_bank_start_year', DEFAULT_START_YEAR)
    test_end_year = config_loader.CONFIG['data_sources'].get('world_bank_end_year', DEFAULT_END_YEAR)

    collect_world_bank_data(
        countries_to_fetch=test_countries,
        indicators_to_fetch=test_indicators,
        start_year=test_start_year,
        end_year=test_end_year
    )

    logger.info("WB_collector.py test run completed.")