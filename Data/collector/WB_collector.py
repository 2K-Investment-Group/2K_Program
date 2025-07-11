import requests
import json
import pandas as pd
import os
import re
import logging
from datetime import datetime
import time # 지연을 위한 time 모듈 추가

# --- (1) 임포트 경로 수정 ---
# config_loader.py는 data/config/ 에 있음
from Data.config import config_loader 

# logger_config.py는 utils/ 에 있음
from utils.logger_config import setup_logging

# --- 로거 객체 생성 (setup_logging은 main.py에서 호출되므로 여기서 직접 호출하지 않음) ---
logger = logging.getLogger(__name__)

# --- (2) 데이터 저장 기본 경로 설정 변경 ---
# BASE_DIR: 프로젝트의 루트 디렉토리 (예: 2K_Program)를 동적으로 찾음
# 이 스크립트가 data/collector/ 에 있으므로, 두 번 상위 디렉토리로 이동해야 합니다.
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

# raw_data 폴더의 경로
RAW_DATA_ROOT = os.path.join(BASE_DIR, "data", "raw_data")

# World Bank 데이터 전용 폴더
WB_DATA_FOLDER = os.path.join(RAW_DATA_ROOT, "world_bank") # 'WB_data' 대신 'world_bank'로 변경 제안


# --- 설정 상수 (config.yaml에서 로드되도록 변경) ---
# WB_API_BASE_URL: config_loader.CONFIG에서 가져올 예정
# START_YEAR, END_YEAR: config_loader.CONFIG에서 가져올 예정
# TOP_INDICATORS: config_loader.CONFIG에서 가져올 예정 (또는 하드코딩 유지 가능)

# config_loader.CONFIG를 직접 사용합니다.
WB_API_BASE_URL = "https://api.worldbank.org/v2" # 월드뱅크 API는 고정이므로 그대로 유지
# 날짜는 main.py에서 설정하거나, config_loader에서 가져올 수 있습니다.
# 여기서는 기본값으로 현재 연도를 사용하고, 수집 함수에서 오버라이드 가능하게 합니다.
DEFAULT_START_YEAR = 1960 
DEFAULT_END_YEAR = datetime.now().year

# 이 지표 목록은 config.yaml로 옮기거나, 현재처럼 여기에 하드코딩할 수 있습니다.
# 여기서는 일단 현재처럼 하드코딩을 유지하고, main 함수에서 config.yaml의 값을 우선적으로 사용하도록 합니다.
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
    "VC.BTL.DETH": "Battle-related deaths (number of people)", # 안정성 지표
}

def ensure_wb_data_folder_exists():
    """
    World Bank 데이터 저장 폴더 (data/raw_data/world_bank)가 없으면 생성합니다.
    """
    if not os.path.exists(WB_DATA_FOLDER):
        os.makedirs(WB_DATA_FOLDER)
        logger.info(f"World Bank 데이터 저장 폴더 '{WB_DATA_FOLDER}'를 생성했습니다.")

# --- 헬퍼 함수 ---
def get_api_response(url, logger, retries=3, delay=1): 
    """API 요청을 보내고 응답을 JSON으로 파싱합니다. 재시도 로직 포함."""
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            logger.warning(f"API 요청 오류 (시도 {attempt+1}/{retries}): {e} - URL: {url}")
            if attempt < retries - 1:
                time.sleep(delay)
        except json.JSONDecodeError as e:
            logger.error(f"JSON 디코딩 오류: {e} - 응답 텍스트: {response.text[:200]}")
            return None
    logger.error(f"API 요청 실패 후 모든 재시도 소진: {url}")
    return None

def clean_filename(name):
    """지표 이름을 파일 시스템에 안전한 이름으로 변환합니다."""
    # 지표 이름에서 특수 문자 제거, 공백을 밑줄로, 소문자로 변환
    name = re.sub(r'[^\w\s.-]', '', name)
    name = re.sub(r'\s+', '_', name).strip('_')
    name = name.lower()
    # 파일명 길이가 너무 길지 않도록 자름 (OS 제한 대비)
    if len(name) > 200:
        name = name[:200]
    return name + ".csv"

# --- 데이터 수집 함수 ---
def fetch_all_countries(logger):
    """모든 국가 목록을 가져옵니다."""
    url = f"{WB_API_BASE_URL}/country?format=json&per_page=1000"
    data = get_api_response(url, logger)
    if data and len(data) > 1 and data[1] is not None:
        # 'Aggregates' (집계 그룹) 제외하고 실제 국가만 필터링
        countries = {item['id']: item['name'] for item in data[1]
                     if item['id'] != 'all' and item['region']['id'] != 'NA' and item['incomeLevel']['id'] != 'NA'}
        logger.info(f"총 {len(countries)}개의 국가 목록을 가져왔습니다 (집계 그룹 제외).")
        return countries
    logger.error("국가 목록을 가져오지 못했습니다.")
    return {}

def fetch_and_save_indicator_data(country_code, country_name, indicator_code, indicator_name, 
                                  start_year=DEFAULT_START_YEAR, end_year=DEFAULT_END_YEAR):
    """
    특정 국가의 특정 지표 데이터를 월드뱅크 API에서 가져와 CSV로 저장합니다.
    페이지네이션을 처리합니다.
    :param country_code: 국가 코드 (예: 'KOR', 'USA')
    :param country_name: 국가 이름
    :param indicator_code: 지표 코드 (예: 'NY.GDP.MKTP.CD')
    :param indicator_name: 지표 이름
    :param start_year: 데이터 시작 연도
    :param end_year: 데이터 종료 연도
    """
    all_data = []
    page = 1
    total_pages = 1
    
    # URL에 start_year와 end_year 적용
    base_indicator_url = f"{WB_API_BASE_URL}/country/{country_code}/indicator/{indicator_code}?format=json&date={start_year}:{end_year}&per_page=1000"

    logger.debug(f"  └─ '{country_name}' ({country_code})의 '{indicator_name}' ({indicator_code}) 데이터 처리 시작...")

    while page <= total_pages:
        url = f"{base_indicator_url}&page={page}"
        data = get_api_response(url, logger)

        if data and len(data) > 1 and data[1] is not None:
            metadata = data[0]
            current_page_data = data[1]

            for item in current_page_data:
                if item['value'] is not None: # 값이 null이 아닌 경우만 추가
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
            time.sleep(0.05) # 각 페이지 요청 사이에 짧은 지연 (API 정책 준수)
        else:
            if page == 1:
                # 첫 페이지부터 데이터 없으면 조용히 넘어감 (너무 많은 로그 방지)
                # logger.debug(f"    └─ '{country_name}'의 '{indicator_name}' 데이터가 없습니다. (코드: {indicator_code})")
                pass 
            else:
                logger.warning(f"    └─ '{country_name}'의 '{indicator_name}' 데이터 페이지 {page}에서 더 이상 데이터가 없거나 오류 발생.")
            break

    if all_data:
        df = pd.DataFrame(all_data)
        df = df.sort_values(by=['country_code', 'year']).reset_index(drop=True)

        # (3) 파일 저장 경로 변경: WB_DATA_FOLDER 하위에 국가별 폴더 생성
        country_save_dir = os.path.join(WB_DATA_FOLDER, country_name.replace(' ', '_').replace('.', '').replace(',', '')) 
        os.makedirs(country_save_dir, exist_ok=True)

        file_name = clean_filename(indicator_name)
        full_save_path = os.path.join(country_save_dir, file_name)

        try:
            df.to_csv(full_save_path, index=False)
            logger.info(f"    └─ '{country_name}'의 '{indicator_name}' 데이터 {len(df)}개 항목 저장 완료.")
            return True
        except Exception as e:
            logger.error(f"    └─ '{country_name}'의 '{indicator_name}' 데이터 저장 중 오류: {e}")
            return False
    else:
        return False

# --- (4) main 함수를 제거하고 collect_world_bank_data 함수로 통합 ---
# 이 파일은 이제 'main.py'에 의해 호출되는 모듈로 작동합니다.
# 직접 실행 시 테스트를 위한 if __name__ == "__main__": 블록만 남깁니다.

def collect_world_bank_data(countries_to_fetch=None, indicators_to_fetch=None,
                            start_year=None, end_year=None):
    """
    World Bank 데이터를 수집하는 주 함수.
    config.yaml에서 설정된 국가 및 지표를 사용하여 데이터를 수집합니다.
    :param countries_to_fetch: 수집할 국가 코드 리스트 (None이면 모든 국가)
    :param indicators_to_fetch: 수집할 지표 코드-이름 딕셔너리 (None이면 DEFAULT_TOP_INDICATORS 사용)
    :param start_year: 수집 시작 연도 (None이면 DEFAULT_START_YEAR 사용)
    :param end_year: 수집 종료 연도 (None이면 DEFAULT_END_YEAR 사용)
    """
    
    # 설정 가져오기 (config.yaml이 우선)
    current_start_year = start_year if start_year is not None else config_loader.CONFIG.get('data_sources', {}).get('world_bank_start_year', DEFAULT_START_YEAR)
    current_end_year = end_year if end_year is not None else config_loader.CONFIG.get('data_sources', {}).get('world_bank_end_year', DEFAULT_END_YEAR)
    
    # config.yaml의 world_bank_indicators를 우선 사용하고, 없으면 DEFAULT_TOP_INDICATORS 사용
    current_indicators = indicators_to_fetch if indicators_to_fetch is not None else \
                         config_loader.CONFIG.get('data_sources', {}).get('world_bank_indicators', DEFAULT_TOP_INDICATORS)

    if not current_indicators:
        logger.warning("수집할 World Bank 지표 목록이 비어 있습니다. 수집을 건너뜁니다.")
        return False

    logger.info("World Bank 데이터 수집 시작.")
    logger.info(f"수집 기간: {current_start_year}년부터 {current_end_year}년까지")
    logger.info(f"총 {len(current_indicators)}개 지표를 수집합니다.")

    # 1. 모든 국가 목록 가져오기 (특정 국가만 지정되지 않은 경우)
    if countries_to_fetch is None:
        countries = fetch_all_countries(logger)
    else:
        # config.yaml에서 country_codes가 제공된 경우 해당 국가만 사용
        all_available_countries = fetch_all_countries(logger) # 전체 국가 목록을 먼저 가져옴
        countries = {code: name for code, name in all_available_countries.items() if code in countries_to_fetch}
        if not countries:
            logger.warning(f"config.yaml에 지정된 국가 코드 {countries_to_fetch} 중 World Bank API에서 찾을 수 있는 국가가 없습니다. 수집을 건너뜁니다.")
            return False

    if not countries:
        logger.critical("국가 목록을 가져오지 못하여 World Bank 데이터 수집을 종료합니다.")
        return False

    total_countries = len(countries)
    total_indicators_to_fetch = len(current_indicators)
    logger.info(f"총 {total_countries}개 국가에 대해 {total_indicators_to_fetch}개 지표를 처리합니다.")

    # 저장 폴더가 있는지 확인
    ensure_wb_data_folder_exists()

    # 2. 각 국가에 대해 선별된 지표 데이터 수집 및 저장
    country_processed_count = 0
    succeeded_data_count = 0

    for country_code, country_name in countries.items():
        country_processed_count += 1
        logger.info(f"\n--- 국가 {country_processed_count}/{total_countries}: '{country_name}' ({country_code}) 데이터 수집 시작 ---")

        indicator_in_country_count = 0
        for indicator_code, indicator_name in current_indicators.items():
            indicator_in_country_count += 1
            logger.info(f"  [ {indicator_in_country_count}/{total_indicators_to_fetch} ] '{indicator_name}' 데이터 시도...")
            
            if fetch_and_save_indicator_data(country_code, country_name, indicator_code, indicator_name,
                                             start_year=current_start_year, end_year=current_end_year):
                succeeded_data_count += 1
            
            # API 호출 지연 시간 (config.yaml에서 가져오도록)
            # WB_API_DELAY_SECONDS = config_loader.CONFIG.get('api_delays', {}).get('world_bank_delay_seconds', 0.1)
            # time.sleep(WB_API_DELAY_SECONDS) # 각 지표 요청 사이에 짧은 지연

        # 각 국가 처리 후 지연 시간 (API 정책 준수 및 부하 줄이기)
        COUNTRY_PROCESSING_DELAY_SECONDS = config_loader.CONFIG.get('api_delays', {}).get('world_bank_country_delay_seconds', 5)
        logger.info(f"국가 '{country_name}' ({country_code}) 데이터 처리 완료. 다음 국가까지 {COUNTRY_PROCESSING_DELAY_SECONDS}초 대기...")
        time.sleep(COUNTRY_PROCESSING_DELAY_SECONDS)

    logger.info(f"\n모든 월드뱅크 데이터 수집 시도 완료. 총 {succeeded_data_count}개의 지표-국가 데이터 세트가 저장되었습니다.")
    return True # 수집 프로세스 완료

if __name__ == "__main__":
    # 이 블록은 WB_collector.py 파일을 직접 실행할 때만 작동합니다 (테스트 목적)
    setup_logging() # 직접 실행 시 로깅 설정 (utils/logger_config.py에서)

    logger.info("WB_collector.py 스크립트 직접 실행 (테스트 목적).")

    # config.yaml의 설정 로드
    # 테스트를 위한 임의의 국가 및 지표 설정 (config.yaml에서 가져올 수도 있음)
    test_countries = config_loader.CONFIG['data_sources'].get('world_bank_countries', ['KOR', 'USA'])
    test_indicators = config_loader.CONFIG['data_sources'].get('world_bank_indicators', DEFAULT_TOP_INDICATORS)
    test_start_year = config_loader.CONFIG['data_sources'].get('world_bank_start_year', DEFAULT_START_YEAR)
    test_end_year = config_loader.CONFIG['data_sources'].get('world_bank_end_year', DEFAULT_END_YEAR)

    # World Bank 데이터 수집 함수 호출
    collect_world_bank_data(
        countries_to_fetch=test_countries,
        indicators_to_fetch=test_indicators,
        start_year=test_start_year,
        end_year=test_end_year
    )

    logger.info("WB_collector.py 테스트 실행 완료.")