import logging
import os
from datetime import datetime

def setup_logging():
    log_dir = "logs"
    # 로그 디렉토리가 없으면 생성 (이미 존재하면 아무것도 하지 않음)
    os.makedirs(log_dir, exist_ok=True)

    # 로그 파일 이름에 '년월일_시분초'를 포함하여 매 실행 시 고유하게 생성
    # 이렇게 하면 같은 날 여러 번 실행해도 각각 다른 로그 파일이 생성됩니다.
    log_filename = datetime.now().strftime(f"{log_dir}/quant_analysis_%Y%m%d_%H%M%S.log")

    # 루트 로거 가져오기
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO) # 기본 로그 레벨 설정 (INFO)

    # 핸들러가 중복으로 추가되는 것을 방지하기 위해 기존 핸들러 제거
    # 특히 Jupyter Notebook이나 스크립트 재실행 시 유용합니다.
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 파일 핸들러 설정
    # 기본 mode는 'a' (append) 입니다. 고유한 파일명이라 굳이 명시 안해도 됨.
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # 콘솔 핸들러 설정
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # 불필요한 라이브러리 로그 필터링 (WARN 레벨 이상만 표시)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('yfinance').setLevel(logging.WARNING)
    logging.getLogger('ccxt').setLevel(logging.WARNING)
    logging.getLogger('psycopg2').setLevel(logging.WARNING)

    # 로깅 설정이 완료되었음을 알리는 메시지 (필요시)
    logger = logging.getLogger(__name__)
    logger.info(f"로깅이 파일 '{log_filename}'에 시작되었습니다.")

# 이 라인은 data_collector.py에서 setup_logging()을 호출하므로,
# logger_config.py 파일 자체에서는 제거하는 것이 좋습니다.
# setup_logging()