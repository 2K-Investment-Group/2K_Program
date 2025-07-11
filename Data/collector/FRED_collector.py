import os
import pandas as pd
from fredapi import Fred
from datetime import datetime, timedelta
import logging
import sys

# --- (1) 임포트 경로 수정 ---
# config_loader.py는 data/config/ 에 있음
from Data.config import config_loader 

# logger_config.py는 utils/ 에 있음
# FRED_collector.py의 setup_logging()을 대체하기 위해 main.py에서 임포트할 수 있도록 변경
from utils.logger_config import setup_logging

# --- 로거 객체 생성 (setup_logging은 main.py에서 호출되므로 여기서 직접 호출하지 않음) ---
# 이 파일에서 로깅을 사용하기 위해 로거 인스턴스를 가져옵니다.
logger = logging.getLogger(__name__)

# --- (2) 데이터 저장 기본 경로 설정 변경 ---
# BASE_DIR: 프로젝트의 루트 디렉토리 (예: 2K_Program)를 동적으로 찾음
# 이 스크립트가 data/collector/ 에 있으므로, 두 번 상위 디렉토리로 이동해야 합니다.
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

# raw_data 폴더의 경로
RAW_DATA_ROOT = os.path.join(BASE_DIR, "data", "raw_data")

# FRED 데이터 전용 폴더
FRED_DATA_FOLDER = os.path.join(RAW_DATA_ROOT, "fred")


def ensure_fred_data_folder_exists():
    """
    FRED 데이터 저장 폴더 (data/raw_data/fred)가 없으면 생성합니다.
    """
    if not os.path.exists(FRED_DATA_FOLDER):
        os.makedirs(FRED_DATA_FOLDER)
        logger.info(f"FRED 데이터 저장 폴더 '{FRED_DATA_FOLDER}'를 생성했습니다.")

def get_fred_api_key_from_config():
    """
    CONFIG 전역 변수에서 FRED API 키를 가져옵니다 (api_keys 섹션 아래).
    """
    api_key = config_loader.CONFIG.get('api_keys', {}).get('fred')
    if not api_key:
        logger.critical("FRED API 키를 가져올 수 없습니다. 'config.yaml'의 'api_keys: fred: YOUR_FRED_API_KEY' 설정을 확인해주세요.")
        # main.py에서 API 키가 없으면 해당 수집을 건너뛰도록 처리할 것이므로, 여기서 sys.exit(1)은 제거합니다.
        return None
    return api_key

def collect_fred_series(series_id, start_date_str=None, end_date_str=None):
    """
    FRED에서 단일 시리즈 데이터를 다운로드하고 지정된 경로에 CSV 파일로 저장합니다.
    이 함수는 main.py에서 series_id와 선택적 날짜를 직접 받아 호출될 수 있도록 설계되었습니다.
    :param series_id: FRED 시리즈 ID (예: 'GDP', 'UNRATE')
    :param start_date_str: 시작 날짜 문자열 (YYYY-MM-DD), 없으면 FRED 기본값 사용
    :param end_date_str: 종료 날짜 문자열 (YYYY-MM-DD), 없으면 오늘 날짜 사용
    """
    api_key = get_fred_api_key_from_config()
    if not api_key:
        return False # API 키가 없으면 수집 실패

    fred = Fred(api_key=api_key)

    logger.info(f"'{series_id}' 데이터 다운로드 시작...")
    
    # 저장될 파일 경로
    # 시리즈 ID에 슬래시나 점이 있을 수 있으므로 파일명에 적합하게 변환
    cleaned_series_id = series_id.replace('/', '_').replace('.', '_')
    file_name = f"{cleaned_series_id}.csv"
    full_save_path = os.path.join(FRED_DATA_FOLDER, file_name)

    # 데이터 저장 폴더가 있는지 확인
    ensure_fred_data_folder_exists()
    logger.info(f"데이터가 '{full_save_path}'에 저장될 예정입니다.")

    # 날짜 유효성 검사 및 설정
    start_date = None
    if start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        except ValueError:
            logger.error(f"'{series_id}': 잘못된 시작 날짜 형식: '{start_date_str}'. YYYY-MM-DD 형식으로 입력해주세요. 전체 데이터 다운로드를 시도합니다.")
            start_date = None # 날짜 파싱 실패 시 전체 데이터 다운로드 시도

    end_date = datetime.now() # 기본값은 현재 날짜
    if end_date_str:
        if end_date_str.lower() == 'latest':
            end_date = datetime.now()
        else:
            try:
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            except ValueError:
                logger.error(f"'{series_id}': 잘못된 종료 날짜 형식: '{end_date_str}'. YYYY-MM-DD 형식으로 입력해주세요. 오늘까지 데이터를 다운로드합니다.")
                end_date = datetime.now()

    try:
        data = fred.get_series(series_id, observation_start=start_date, observation_end=end_date)
        
        if data is None or data.empty:
            logger.warning(f"'{series_id}' 데이터가 비어있습니다. FRED에서 데이터를 가져오지 못했습니다. 시리즈 ID를 확인하거나 지정된 기간에 데이터가 없을 수 있습니다.")
            return False
        
        # Series를 DataFrame으로 변환하고, 날짜 인덱스를 컬럼으로 리셋
        data_df = pd.DataFrame(data)
        data_df.index.name = 'Date'
        data_df.columns = ['Value'] # FRED 데이터는 보통 단일 값으로 구성됩니다.

        data_df.to_csv(full_save_path)
        logger.info(f"'{series_id}' 데이터가 '{full_save_path}'에 성공적으로 저장되었습니다.")
        return True

    except Exception as e:
        logger.error(f"'{series_id}' 데이터 다운로드 또는 저장 중 예상치 못한 오류 발생: {e}", exc_info=True)
        logger.error("FRED API 키가 유효한지, 네트워크 연결 상태, 그리고 FRED 시리즈 ID가 정확한지 확인해주세요.")
        return False

# --- (3) main 함수를 제거하고 collect_fred_series 함수로 통합 ---
# 이 파일은 이제 'main.py'에 의해 호출되는 모듈로 작동합니다.
# 직접 실행 시 테스트를 위한 if __name__ == "__main__": 블록만 남깁니다.

if __name__ == "__main__":
    # 이 블록은 FRED_collector.py 파일을 직접 실행할 때만 작동합니다 (테스트 목적)
    setup_logging() # 직접 실행 시 로깅 설정

    logger.info("FRED_collector.py 스크립트 직접 실행 (테스트 목적).")

    # config.yaml에서 테스트에 사용할 FRED 시리즈 ID 가져오기
    test_fred_series_ids = config_loader.CONFIG['data_sources'].get('fred_series', [])

    if not test_fred_series_ids:
        logger.warning("config.yaml에 'data_sources' 아래 'fred_series' 목록이 비어 있습니다. FRED 테스트 실행을 건너뜁니다.")
        sys.exit(0) # 테스트할 데이터가 없으면 종료

    ensure_fred_data_folder_exists() # 테스트 전에 폴더 생성 확인

    total_datasets = len(test_fred_series_ids)
    succeeded_count = 0

    # config.yaml의 각 시리즈 ID에 대해 수집 함수 호출
    for i, series_id in enumerate(test_fred_series_ids):
        logger.info(f"\n--- [{i+1}/{total_datasets}] '{series_id}' 다운로드 시도 ---")
        # config.yaml에 start_date, end_date가 정의되어 있다면 여기서 가져와서 전달할 수도 있습니다.
        # 현재 collect_fred_series 함수는 날짜 인자를 옵션으로 받습니다.
        if collect_fred_series(series_id): 
            succeeded_count += 1
        else:
            logger.error(f"--- '{series_id}' 다운로드 실패 ---")

    logger.info(f"\n--- FRED_collector.py 테스트 실행 완료. 총 {total_datasets}개 중 {succeeded_count}개 성공. ---")