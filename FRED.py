import os
import pandas as pd
from fredapi import Fred
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import sys

# --- config_loader.py에서 CONFIG를 가져옵니다. ---
# 이 파일이 .env도 로드하고 config.yaml에서 최종 설정을 가져옵니다.
from Data.config.config_loader import CONFIG 

# --- 로깅 설정 ---
LOG_DIR = 'logs'

def setup_logging():
    """
    로깅 설정을 초기화하고 로거 객체를 반환합니다.
    """
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # 중복 핸들러 방지
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        log_file_name = datetime.now().strftime('fred_data_%Y%m%d_%H%M%S.log')
        log_file_path = os.path.join(LOG_DIR, log_file_name)
        file_handler = RotatingFileHandler(log_file_path, maxBytes=10 * 1024 * 1024, backupCount=5)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.info("FRED 데이터 수집 로깅이 시작되었습니다.")
    return logger

# --- 데이터 저장 기준 폴더 ---
# config.yaml의 data_storage.base_dir 설정을 우선 사용하고, 없으면 기본값으로 "FRED_Data"를 사용합니다.
DATA_BASE_DIR = CONFIG.get('data_storage', {}).get('base_dir', "FRED_Data")

def ensure_data_base_dir_exists():
    """
    FRED 데이터 저장 폴더가 없으면 생성합니다.
    """
    if not os.path.exists(DATA_BASE_DIR):
        os.makedirs(DATA_BASE_DIR)
        logger.info(f"기본 FRED 데이터 저장 폴더 '{DATA_BASE_DIR}'를 생성했습니다.")

def get_fred_api_key_from_config():
    """
    CONFIG 전역 변수에서 FRED API 키를 가져옵니다 (api_keys 섹션 아래).
    """
    api_key = CONFIG.get('api_keys', {}).get('fred')
    if not api_key:
        logger.critical("FRED API 키를 가져올 수 없습니다. 'config.yaml'의 'api_keys: fred: FRED_API_KEY' 설정과 '.env' 파일의 FRED_API_KEY 변수를 확인해주세요.")
        sys.exit(1) # API 키가 없으면 프로그램 종료
    return api_key

def download_and_save_fred_data(dataset_info):
    """
    FRED에서 데이터를 다운로드하고 지정된 경로에 CSV 파일로 저장합니다.
    config.yaml의 dataset_info 딕셔너리에서 모든 필요한 정보를 가져옵니다.
    :param dataset_info: 딕셔너리 형태의 데이터셋 정보 (name, series_id, file_name, path, start_date, end_date 포함)
    """
    api_key = get_fred_api_key_from_config()
    fred = Fred(api_key=api_key)

    series_id = dataset_info['series_id']
    name = dataset_info.get('name', series_id)
    file_name = dataset_info.get('file_name', f"{series_id.replace('/', '_').replace('.', '_')}.csv")
    relative_path = dataset_info.get('path', 'Manual_Downloads') # 수동 다운로드 기본 경로 (자동화에서는 잘 사용 안 됨)
    
    # config.yaml에서 날짜 정보 가져오기
    start_date_str = dataset_info.get('start_date')
    end_date_str = dataset_info.get('end_date')

    full_save_dir = os.path.join(DATA_BASE_DIR, relative_path)
    full_save_path = os.path.join(full_save_dir, file_name)

    os.makedirs(full_save_dir, exist_ok=True)

    logger.info(f"'{name}' ({series_id}) 데이터 다운로드 시작...")
    logger.info(f"저장될 경로: {full_save_path}")

    # 날짜 유효성 검사 및 설정
    start_date = None
    if start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        except ValueError:
            logger.error(f"'{name}' ({series_id}): 잘못된 시작 날짜 형식: '{start_date_str}'. YYYY-MM-DD 형식으로 입력해주세요. 전체 데이터 다운로드를 시도합니다.")
            start_date = None # 날짜 파싱 실패 시 전체 데이터 다운로드 시도

    end_date = datetime.now() # 기본값은 현재 날짜
    if end_date_str:
        if end_date_str.lower() == 'latest':
            end_date = datetime.now()
        else:
            try:
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            except ValueError:
                logger.error(f"'{name}' ({series_id}): 잘못된 종료 날짜 형식: '{end_date_str}'. YYYY-MM-DD 형식으로 입력해주세요. 오늘까지 데이터를 다운로드합니다.")
                end_date = datetime.now()

    try:
        data = fred.get_series(series_id, observation_start=start_date, observation_end=end_date)
        
        if data is None or data.empty:
            logger.warning(f"'{name}' ({series_id}) 데이터가 비어있습니다. FRED에서 데이터를 가져오지 못했습니다. 시리즈 ID를 확인하거나 지정된 기간에 데이터가 없을 수 있습니다.")
            return False
        
        # Series를 DataFrame으로 변환하고, 날짜 인덱스를 컬럼으로 리셋
        data_df = pd.DataFrame(data)
        data_df.index.name = 'Date'
        data_df.columns = ['Value'] # FRED 데이터는 보통 단일 값으로 구성됩니다.

        data_df.to_csv(full_save_path)
        logger.info(f"'{name}' 데이터가 '{full_save_path}'에 성공적으로 저장되었습니다.")
        return True

    except Exception as e:
        logger.error(f"'{name}' ({series_id}) 데이터 다운로드 또는 저장 중 예상치 못한 오류 발생: {e}", exc_info=True)
        logger.error("FRED API 키가 유효한지, 네트워크 연결 상태, 그리고 FRED 시리즈 ID가 정확한지 확인해주세요.")
        return False

def main():
    global logger
    logger = setup_logging()

    logger.info("FRED 자동 데이터 수집 스크립트 시작.")
    ensure_data_base_dir_exists() # 데이터 저장 폴더 확인

    # config.yaml에서 fred_datasets 목록을 가져옵니다.
    fred_datasets_to_download = CONFIG.get('fred_datasets', [])

    if not fred_datasets_to_download:
        logger.warning("config.yaml에 'fred_datasets' 섹션이 없거나 비어 있습니다. 다운로드할 데이터가 없습니다.")
        return

    total_datasets = len(fred_datasets_to_download)
    succeeded_count = 0

    for i, dataset_info in enumerate(fred_datasets_to_download):
        logger.info(f"\n--- [{i+1}/{total_datasets}] '{dataset_info.get('name', dataset_info['series_id'])}' 다운로드 시도 ---")
        if download_and_save_fred_data(dataset_info):
            succeeded_count += 1
        else:
            logger.error(f"--- '{dataset_info.get('name', dataset_info['series_id'])}' 다운로드 실패 ---")

    logger.info(f"\n--- FRED 자동 데이터 수집 완료. 총 {total_datasets}개 중 {succeeded_count}개 성공. ---")

if __name__ == "__main__":
    main()