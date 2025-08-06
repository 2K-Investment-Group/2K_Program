import requests
import json
import pandas as pd
import os
import re
import logging
from datetime import datetime
import time
import sys
from sqlalchemy.types import String, Float, Date, Integer # SQLAlchemy 타입 명시적으로 임포트

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from Data_Collection.config import config_loader
from utils.logger_config import setup_logging
from Data_Collection.storage.db_utils import get_db_engine

logger = logging.getLogger(__name__)

WB_API_BASE_URL = "https://api.worldbank.org/v2"

def get_api_response(url, logger, retries=3, delay=1):
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status() # 4xx, 5xx 에러 발생 시 예외 발생
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            logger.warning(f"API request error (Attempt {attempt+1}/{retries}): {e} - URL: {url}")
            if attempt < retries - 1:
                time.sleep(delay)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error: {e} - Response text: {response.text[:200]}... URL: {url}")
            return None # JSON 디코딩 오류는 재시도해도 해결되지 않을 가능성이 높으므로 바로 종료
    logger.error(f"All retries exhausted for API request: {url}")
    return None

def fetch_all_countries(logger):
    """
    World Bank API에서 모든 국가 목록을 가져옵니다.
    집계 그룹(예: 'World', 'Euro Area')을 제외하고 실제 국가만 반환합니다.
    """
    url = f"{WB_API_BASE_URL}/country?format=json&per_page=500" # per_page를 500으로 늘려 한 번에 더 많이 가져오기
    data = get_api_response(url, logger)
    if data and len(data) > 1 and data[1] is not None:
        countries = {item['id']: item['name'] for item in data[1]
                     if item['id'] != 'all' and item['region']['id'] != 'NA' and item['incomeLevel']['id'] != 'NA'}
        logger.info(f"Fetched a list of {len(countries)} countries (excluding aggregate groups).")
        return countries
    logger.error("Failed to fetch country list from World Bank API.")
    return {}

def fetch_and_save_indicator_data(country_code, country_name, indicator_code, indicator_name,
                                  start_year, end_year): # start_year, end_year는 이제 인자로 필수로 받음
    """
    World Bank API에서 특정 국가-지표 데이터를 가져와 DataFrame으로 처리하고 데이터베이스에 저장합니다.
    """
    all_data = []
    page = 1
    total_pages = 1
    
    # World Bank API는 date=start:end 형식으로 사용
    base_indicator_url = f"{WB_API_BASE_URL}/country/{country_code}/indicator/{indicator_code}?format=json&date={start_year}:{end_year}&per_page=1000"

    logger.debug(f"    └─ Fetching data for '{country_name}' ({country_code}) - '{indicator_name}' ({indicator_code})...")

    while page <= total_pages:
        url = f"{base_indicator_url}&page={page}"
        data = get_api_response(url, logger)

        if data and len(data) > 1 and data[1] is not None:
            metadata = data[0]
            current_page_data = data[1]

            for item in current_page_data:
                # 'value'가 None이 아니고, 'date'가 유효한 경우만 추가
                if item['value'] is not None and item.get('date') is not None:
                    try:
                        year_val = int(item['date'])
                        value_val = float(item['value'])
                        all_data.append({
                            'country_name': item['country']['value'],
                            'country_code': item['countryiso3code'],
                            'indicator_name': item['indicator']['value'],
                            'indicator_code': item['indicator']['id'],
                            'year': year_val,
                            'value': value_val
                        })
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Skipping malformed data entry for '{indicator_name}' in '{country_name}': {item}. Error: {e}")
            
            total_pages = metadata['pages']
            page += 1
            time.sleep(0.05) # API 호출 간 짧은 딜레이 추가
        else:
            if page == 1: # 첫 페이지부터 데이터가 없는 경우
                logger.info(f"    └─ No data found for '{country_name}' - '{indicator_name}' in the specified period.")
            else: # 중간에 데이터가 없거나 오류 발생 시
                logger.warning(f"    └─ No more data or an error occurred on page {page} for '{country_name}' - '{indicator_name}'.")
            break # 더 이상 페이지를 요청할 필요 없음

    if not all_data:
        logger.info(f"    └─ No valid data collected for '{country_name}' - '{indicator_name}'.")
        return False

    df = pd.DataFrame(all_data)
    
    # 'year' 컬럼을 기반으로 'date' 컬럼 생성 (YYYY-MM-01 형식)
    df['date'] = pd.to_datetime(df['year'].astype(str) + '-01-01', errors='coerce')
    
    # 필요한 컬럼만 선택하고 순서 정렬
    df = df[['country_name', 'country_code', 'indicator_name', 'indicator_code', 'date', 'value']]
    df = df.sort_values(by=['country_code', 'indicator_code', 'date']).reset_index(drop=True)

    # DB Engine
    engine = get_db_engine()
    if not engine:
        logger.error(f"Failed to get DB engine for {country_code}-{indicator_code}. Cannot save to database.")
        return False

    table_name = "world_bank_indicators_raw"
    try:
        # dtype 매핑에 SQLAlchemy 타입 객체 사용
        df.to_sql(table_name, engine, if_exists='append', index=False, dtype={
            'country_name': String(255),
            'country_code': String(10),
            'indicator_name': String(500), # 지표 이름이 길 수 있으므로 넉넉하게 설정
            'indicator_code': String(50),
            'date': Date,
            'value': Float # TimescaleDB의 Numeric과 호환
        })
        logger.info(f"    └─ Successfully saved {len(df)} entries for '{indicator_name}' in '{country_name}' to DB.")
        return True
    except Exception as e:
        error_str = str(e)
        if "duplicate key value violates unique constraint" in error_str or "UNIQUE constraint failed" in error_str:
            logger.warning(f"    └─ Data for '{country_name}' - '{indicator_name}' (some dates) already exists in '{table_name}'. New data appended, existing dates skipped/not updated. Error: {e}")
            return True # 이미 존재하는 데이터는 성공으로 간주
        else:
            logger.error(f"    └─ Error saving '{indicator_name}' data for '{country_name}' to database: {e}", exc_info=True)
            return False
    finally:
        if engine:
            engine.dispose()

def collect_world_bank_data(countries_to_fetch=None, indicators_to_fetch=None,
                            start_year=None, end_year=None):
    """
    World Bank 데이터를 종합적으로 수집하는 메인 함수.
    config.yaml 또는 인자로 전달된 설정을 사용하여 데이터를 가져옵니다.
    """
    
    # config.yaml에서 시작/종료 연도 설정 로드. 없으면 현재 연도 기준으로 처리.
    current_start_year = start_year if start_year is not None else config_loader.CONFIG.get('data_sources', {}).get('world_bank_start_year', datetime.now().year - 5) # 기본값 5년 전
    current_end_year = end_year if end_year is not None else config_loader.CONFIG.get('data_sources', {}).get('world_bank_end_year', datetime.now().year)

    # config.yaml에서 'world_bank_indicators' 설정 로드
    configured_indicators_raw = config_loader.CONFIG.get('data_sources', {}).get('world_bank_indicators', [])
    
    current_indicators = {}
    if isinstance(configured_indicators_raw, list): # config에 리스트 형태로 정의된 경우
        current_indicators = {item['indicator_id']: item.get('name', item['indicator_id']) for item in configured_indicators_raw if 'indicator_id' in item}
    elif isinstance(configured_indicators_raw, dict): # config에 딕셔너리 형태로 정의된 경우 (선호되지 않지만 호환성을 위해)
        current_indicators = configured_indicators_raw
    
    # 함수 인자로 indicators_to_fetch가 명시적으로 전달되면 그것을 우선적으로 사용
    if indicators_to_fetch is not None:
        if isinstance(indicators_to_fetch, list):
            current_indicators = {item['indicator_id']: item.get('name', item['indicator_id']) for item in indicators_to_fetch if 'indicator_id' in item}
        elif isinstance(indicators_to_fetch, dict):
            current_indicators = indicators_to_fetch
        # 만약 인자가 None이 아닌데 유효한 딕셔너리/리스트가 아니면 오류 발생 (여기서는 처리 생략)


    if not current_indicators:
        logger.error("World Bank indicator list (from config.yaml or arguments) is empty. Cannot proceed with data collection.")
        return False

    logger.info("Starting World Bank data collection.")
    logger.info(f"Collection period: From {current_start_year} to {current_end_year}")
    logger.info(f"Collecting {len(current_indicators)} indicators.")

    # 국가 목록 처리
    all_available_countries = fetch_all_countries(logger)
    if not all_available_countries:
        logger.critical("Failed to fetch country list. Terminating World Bank data collection.")
        return False

    countries_to_process = {}
    # countries_to_fetch가 None이면 config에서 'world_bank_countries'를 가져옴.
    # config에도 없으면 빈 리스트로 초기화되어 모든 국가를 가져오지 않도록 함.
    countries_from_config = config_loader.CONFIG.get('data_sources', {}).get('world_bank_countries', [])

    # 우선순위: 함수 인자 > config.yaml
    final_countries_list = countries_to_fetch if countries_to_fetch is not None else countries_from_config

    if not final_countries_list: # config나 인자에 국가가 지정되지 않았다면
        logger.warning("No specific countries defined in config.yaml or arguments. To collect all available countries, explicitly set `world_bank_countries: ['all']` or similar in config, or provide a list of codes.")
        logger.warning("Skipping country-specific data collection as no target countries were specified.")
        # 만약 'all'을 넣으면 모든 국가를 가져오도록 처리할 수도 있지만, 여기서는 명시된 국가만 처리.
        return False


    for code in final_countries_list:
        if code in all_available_countries:
            countries_to_process[code] = all_available_countries[code]
        else:
            logger.warning(f"Specified country code '{code}' not found in World Bank API. Skipping.")
    
    if not countries_to_process:
        logger.warning(f"No valid countries found to process among the specified: {final_countries_list}. Skipping collection.")
        return False


    total_countries = len(countries_to_process)
    total_indicators_to_fetch = len(current_indicators)
    logger.info(f"Processing {total_indicators_to_fetch} indicators for {total_countries} countries.")

    country_processed_count = 0
    overall_succeeded_data_count = 0

    for country_code, country_name in countries_to_process.items():
        country_processed_count += 1
        logger.info(f"\n--- Country {country_processed_count}/{total_countries}: Starting data collection for '{country_name}' ({country_code}) ---")

        indicator_in_country_count = 0
        for indicator_code, indicator_name in current_indicators.items():
            indicator_in_country_count += 1
            logger.info(f"    [ {indicator_in_country_count}/{total_indicators_to_fetch} ] Attempting data for '{indicator_name}' ({indicator_code})...")
            
            if fetch_and_save_indicator_data(country_code, country_name, indicator_code, indicator_name,
                                             start_year=current_start_year, end_year=current_end_year):
                overall_succeeded_data_count += 1
            
            INDICATOR_PROCESSING_DELAY_SECONDS = config_loader.CONFIG.get('api_delays', {}).get('world_bank_indicator_delay_seconds', 0.1) # 지표별 딜레이 추가
            time.sleep(INDICATOR_PROCESSING_DELAY_SECONDS)

        COUNTRY_PROCESSING_DELAY_SECONDS = config_loader.CONFIG.get('api_delays', {}).get('world_bank_country_delay_seconds', 5)
        logger.info(f"Finished processing data for country '{country_name}' ({country_code}). Waiting {COUNTRY_PROCESSING_DELAY_SECONDS} seconds until next country...")
        time.sleep(COUNTRY_PROCESSING_DELAY_SECONDS)

    logger.info(f"\nAll World Bank data collection attempts completed. Total {overall_succeeded_data_count} indicator-country datasets saved.")
    return True

if __name__ == "__main__":
    setup_logging()

    logger.info("Running WB_collector.py script directly (for testing purposes).")

    collect_world_bank_data()

    logger.info("WB_collector.py test run completed.")