import requests
import json
import pandas as pd
import os
import re
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import time # 지연을 위한 time 모듈 추가

# --- 설정 상수 ---
WB_API_BASE_URL = "https://api.worldbank.org/v2"
DATA_BASE_DIR = "WB_data" # 데이터를 저장할 최상위 폴더
START_YEAR = 1960
END_YEAR = datetime.now().year

# 월드뱅크의 가장 중요한 지표 20가지 (선별)
# 지표 코드는 월드뱅크 카탈로그에서 확인된 공식 코드입니다.
TOP_INDICATORS = {
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


# --- 로깅 설정 ---
LOG_DIR = 'logs'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def setup_logging():
    log_file_name = datetime.now().strftime('WB_top20_indicators_%Y%m%d_%H%M%S.log')
    log_file_path = os.path.join(LOG_DIR, log_file_name)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(log_file_path, maxBytes=10*1024*1024, backupCount=5)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info(f"로깅이 파일 '{log_file_path}'에 시작되었습니다.")
    return logger

# --- 헬퍼 함수 ---
def get_api_response(url, logger, retries=3, delay=1): # 지연 시간을 1초로 줄여도 됨
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
    name = re.sub(r'[^\w\s.-]', '', name)
    name = re.sub(r'\s+', '_', name).strip('_')
    name = name.lower()
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
                     if item['id'] != 'all' and item['region']['id'] != 'NA'} # 'NA'는 'Not Applicable' 같은 집계 그룹임
        logger.info(f"총 {len(countries)}개의 국가 목록을 가져왔습니다 (집계 그룹 제외).")
        return countries
    logger.error("국가 목록을 가져오지 못했습니다.")
    return {}

def fetch_and_save_indicator_data(country_code, country_name, indicator_code, indicator_name, logger):
    """
    특정 국가의 특정 지표 데이터를 월드뱅크 API에서 가져와 CSV로 저장합니다.
    페이지네이션을 처리합니다.
    """
    all_data = []
    page = 1
    total_pages = 1
    base_indicator_url = f"{WB_API_BASE_URL}/country/{country_code}/indicator/{indicator_code}?format=json&date={START_YEAR}:{END_YEAR}&per_page=1000"

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
            time.sleep(0.05) # 각 페이지 요청 사이에 짧은 지연 (선택 사항)
        else:
            if page == 1:
                # logger.debug(f"    └─ '{country_name}'의 '{indicator_name}' 데이터가 없습니다. (코드: {indicator_code})")
                pass # 첫 페이지부터 데이터 없으면 조용히 넘어감 (너무 많은 로그 방지)
            else:
                logger.warning(f"    └─ '{country_name}'의 '{indicator_name}' 데이터 페이지 {page}에서 더 이상 데이터가 없거나 오류 발생.")
            break

    if all_data:
        df = pd.DataFrame(all_data)
        df = df.sort_values(by=['country_code', 'year']).reset_index(drop=True)

        country_save_dir = os.path.join(DATA_BASE_DIR, country_name.replace(' ', '_').replace('.', '')) # 점(.) 제거
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

# --- 메인 함수 ---
def main():
    logger = setup_logging()
    logger.info("WB_top20_indicators.py 스크립트 시작: 월드뱅크 핵심 지표 20가지 수집")
    logger.info(f"수집 기간: {START_YEAR}년부터 {END_YEAR}년까지")
    logger.info(f"총 {len(TOP_INDICATORS)}개 핵심 지표를 수집합니다.")


    # 1. 모든 국가 목록 가져오기
    countries = fetch_all_countries(logger)
    if not countries:
        logger.critical("국가 목록을 가져오지 못하여 스크립트를 종료합니다.")
        return

    total_countries = len(countries)
    total_indicators_to_fetch = len(TOP_INDICATORS)
    logger.info(f"총 {total_countries}개 국가에 대해 {total_indicators_to_fetch}개 지표를 처리합니다.")


    # 2. 각 국가에 대해 선별된 지표 데이터 수집 및 저장
    country_count = 0
    for country_code, country_name in countries.items():
        country_count += 1
        logger.info(f"\n--- 국가 {country_count}/{total_countries}: '{country_name}' ({country_code}) 데이터 수집 시작 ---")

        indicator_processed_count = 0
        for indicator_code, indicator_name in TOP_INDICATORS.items():
            indicator_processed_count += 1
            logger.info(f"  [ {indicator_processed_count}/{total_indicators_to_fetch} ] '{indicator_name}' 데이터 시도...")
            fetch_and_save_indicator_data(country_code, country_name, indicator_code, indicator_name, logger)
            time.sleep(0.01) # 각 지표 요청 사이에 짧은 지연 (선택 사항, 너무 빠르면 문제될 수 있음)

    logger.info("\n모든 월드뱅크 핵심 지표 데이터 수집 시도 완료.")

if __name__ == "__main__":
    main()