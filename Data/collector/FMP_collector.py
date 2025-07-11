import yfinance as yf
import requests
import psycopg2
from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
import logging
import time

# --- (1) 임포트 경로 수정 ---
# 현재 파일 (FMP_collector.py)이 data/collector/ 에 있다고 가정
# config_loader.py는 data/config/ 에 있음
from Data.config import config_loader    # 프로젝트 루트(2K_Program)가 Python Path에 있을 경우

# logger_config.py는 utils/ 에 있음
from utils.logger_config import setup_logging # 프로젝트 루트(2K_Program)가 Python Path에 있을 경우

setup_logging()
logger = logging.getLogger(__name__)

# --- (2) 데이터 저장 기본 경로 설정 변경 ---
# BASE_DIR: 프로젝트의 루트 디렉토리 (예: 2K_Program)를 동적으로 찾음
# 이 스크립트가 data/collector/ 에 있으므로, 두 번 상위 디렉토리로 이동해야 합니다.
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

# raw_data 폴더의 경로
RAW_DATA_ROOT = os.path.join(BASE_DIR, "data", "raw_data")

# YFinance 데이터 전용 폴더 (새로 추가, 기존 Data_YahooFinance 대체)
# yfinance는 AlphaVantage와 유사한 주식 데이터이므로 AlphaVantage와 같은 구조를 따르도록 제안
# 또는 raw_data/yfinance/ 로 별도 폴더를 가질 수도 있습니다. 여기서는 alpha_vantage와 통합을 고려합니다.
# 하지만 FMP_collector에서 yfinance를 사용하므로, raw_data/fmp_yfinance/ 혹은 raw_data/yfinance/ 를 제안합니다.
# 일단 raw_data/yfinance/ 에 저장하는 것으로 합니다.
YFINANCE_DATA_FOLDER = os.path.join(RAW_DATA_ROOT, "yfinance")

# FMP 데이터 전용 폴더 (기존 Data_FMP 대체)
FMP_DATA_FOLDER = os.path.join(RAW_DATA_ROOT, "fmp")

# CCXT 관련 경로는 이 파일에서 제거합니다. CCXT는 AlphaVantage_collector.py에서 처리합니다.
# DATA_FOLDER_CCXT = "Data_CCXT" # -> 이 부분은 제거됩니다.

def ensure_data_folder_exists(folder_path):
    """지정된 데이터 폴더가 없으면 생성합니다."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        logger.info(f"'{folder_path}' 폴더를 생성했습니다.")

# --- Database Connection ---
def get_db_connection():
    """데이터베이스 연결 객체를 반환합니다."""
    try:
        # config_loader.CONFIG를 직접 사용합니다.
        conn = psycopg2.connect(
            host=config_loader.CONFIG['database']['host'],
            database=config_loader.CONFIG['database']['dbname'],
            user=config_loader.CONFIG['database']['user'],
            password=config_loader.CONFIG['database']['password'],
            port=config_loader.CONFIG['database']['port']
        )
        return conn
    except Exception as e:
        logger.error(f"데이터베이스 연결 오류: {e}")
        raise

# --- Stock OHLCV Data Collection (YFinance 기반) ---
def collect_and_save_stock_ohlcv_yfinance(symbol, start_date, end_date):
    logger.info(f"[{symbol}] YFinance에서 주식 OHLCV 데이터 수집 시작 (기간: {start_date} ~ {end_date})...")
    
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception:
        logger.warning(f"[{symbol}] 주식 OHLCV 데이터베이스 연결에 실패하여 DB 저장을 건너뜁니다. 파일 저장만 시도합니다.")

    try:
        data = yf.download(symbol, start=start_date, end=end_date)
        if data.empty:
            logger.warning(f"[{symbol}] 지정된 기간 내에 YFinance에서 주식 데이터를 찾을 수 없습니다. DB 및 CSV 저장을 건너뜁니다.")
            return

        ohlcv_records_for_db = []
        ohlcv_records_for_csv = []
        
        for index, row in data.iterrows():
            trade_date = index.to_pydatetime().date()
            
            open_price = float(row['Open']) if pd.notna(row['Open']) else None
            high_price = float(row['High']) if pd.notna(row['High']) else None
            low_price = float(row['Low']) if pd.notna(row['Low']) else None
            close_price = float(row['Close']) if pd.notna(row['Close']) else None
            volume = int(row['Volume']) if pd.notna(row['Volume']) else None

            ohlcv_records_for_db.append((
                symbol, trade_date, open_price, high_price, low_price, close_price, volume
            ))
            ohlcv_records_for_csv.append({
                'symbol': symbol, 'timestamp': trade_date, 'open': open_price,
                'high': high_price, 'low': low_price, 'close': close_price, 'volume': volume
            })
        
        # --- 1. 데이터베이스에 저장 ---
        if conn and cur:
            if ohlcv_records_for_db:
                try:
                    db_insert_count = 0
                    for record in ohlcv_records_for_db:
                        cur.execute("""
                            INSERT INTO stock_ohlcv (
                                symbol, timestamp, open, high, low, close, volume
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (symbol, timestamp) DO UPDATE SET 
                                open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                                close = EXCLUDED.close, volume = EXCLUDED.volume;
                        """, record)
                        db_insert_count += 1
                    conn.commit()
                    logger.info(f"[{symbol}] YFinance OHLCV 데이터 {db_insert_count}개를 데이터베이스에 성공적으로 저장했습니다.")
                except Exception as e:
                    logger.error(f"[{symbol}] OHLCV 데이터베이스 저장 중 오류: {e}", exc_info=True)
                    conn.rollback() 
            else:
                logger.warning(f"[{symbol}] 수집된 OHLCV 데이터가 없어 데이터베이스에 저장하지 않았습니다.")
        else:
            logger.warning(f"[{symbol}] 데이터베이스 연결 문제로 OHLCV 데이터를 DB에 저장하지 않았습니다.")

        # --- 2. CSV 파일에 저장 ---
        if ohlcv_records_for_csv:
            df_ohlcv = pd.DataFrame(ohlcv_records_for_csv)
            df_ohlcv['timestamp'] = pd.to_datetime(df_ohlcv['timestamp']) 
            df_ohlcv = df_ohlcv.sort_values(by='timestamp').reset_index(drop=True) 
            
            # (3) 파일 저장 경로 변경: YFINANCE_DATA_FOLDER 사용
            # YFinance도 AlphaVantage처럼 거래소/산업분야 구조를 가질 수 있지만,
            # 여기서는 편의상 symbol_ohlcv.csv로 바로 저장합니다.
            # 만약 더 복잡한 구조가 필요하면 AlphaVantage_collector의 로직을 참고하세요.
            file_path = os.path.join(YFINANCE_DATA_FOLDER, f"{symbol}_ohlcv.csv")
            ensure_data_folder_exists(YFINANCE_DATA_FOLDER) # 상위 폴더가 확실히 존재하도록
            
            df_ohlcv.to_csv(file_path, index=False) 
            logger.info(f"[{symbol}] YFinance OHLCV 데이터 {len(df_ohlcv)}개를 '{file_path}'에 성공적으로 저장했습니다.")
        else:
            logger.warning(f"[{symbol}] 수집된 OHLCV 데이터가 없어 CSV 파일로 저장하지 않았습니다.")

    except Exception as e:
        logger.error(f"[{symbol}] YFinance OHLCV 데이터 수집/저장 중 예상치 못한 오류: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Financials Data Collection (Financial Modeling Prep) ---
def collect_and_save_financials_fmp(symbol, api_key): # api_key를 직접 받도록 변경
    logger.info(f"[{symbol}] FMP에서 재무제표 데이터 수집 시작...")
    
    if not api_key:
        logger.warning(f"[{symbol}] FMP API 키를 찾을 수 없습니다. 재무제표 수집을 건너뜁니다.")
        return

    url_income = f"https://financialmodelingprep.com/api/v3/income-statement/{symbol}?period=quarter&limit=100&apikey={api_key}"
    url_balance = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}?period=quarter&limit=100&apikey={api_key}"
    url_cashflow = f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{symbol}?period=quarter&limit=100&apikey={api_key}"

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception:
        logger.warning(f"[{symbol}] 재무제표 데이터베이스 연결에 실패하여 DB 저장을 건너뜁니다. 파일 저장만 시도합니다.")

    try:
        # API 호출 및 응답 처리
        income_data = []
        balance_data = []
        cashflow_data = []

        # 각 API 호출에 대한 오류 처리 및 로깅 강화
        def fetch_json(url, statement_type):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status() # HTTP 오류 (4xx, 5xx) 발생 시 예외
                data = response.json()
                if not data:
                    logger.warning(f"[{symbol}] FMP {statement_type}에서 데이터가 비어 있습니다. URL: {url}")
                return data
            except requests.exceptions.RequestException as e:
                logger.error(f"[{symbol}] FMP {statement_type} API 요청 오류 ({url}): {e}")
                return []
            except requests.exceptions.JSONDecodeError as e:
                logger.error(f"[{symbol}] FMP {statement_type} JSON 파싱 오류 ({url}): {e} - 응답: {response.text[:200]}...")
                return []
            except Exception as e:
                logger.error(f"[{symbol}] FMP {statement_type} 데이터 가져오기 중 예상치 못한 오류 ({url}): {e}", exc_info=True)
                return []

        income_data = fetch_json(url_income, 'income statement')
        time.sleep(1) # API 호출 간 지연
        balance_data = fetch_json(url_balance, 'balance sheet')
        time.sleep(1) # API 호출 간 지연
        cashflow_data = fetch_json(url_cashflow, 'cash flow statement')

        combined_financials = {}
        # 데이터를 결합할 때, key가 없는 경우도 처리 (get 사용)
        for item in income_data:
            date_key = item.get('date') or item.get('fillingDate')
            if date_key:
                combined_financials[date_key] = {**item}

        for item in balance_data:
            date_key = item.get('date') or item.get('fillingDate')
            if date_key:
                combined_financials.setdefault(date_key, {}).update(item)
        for item in cashflow_data:
            date_key = item.get('date') or item.get('fillingDate')
            if date_key:
                combined_financials.setdefault(date_key, {}).update(item)

        if not combined_financials:
            logger.warning(f"[{symbol}] FMP에서 통합된 재무제표 데이터를 찾을 수 없습니다. API 문제 또는 데이터 없음.")
            return

        financial_records_for_db = []
        financial_records_for_csv = []

        for date_str, statement in combined_financials.items():
            try:
                report_date = datetime.strptime(date_str.split('T')[0], '%Y-%m-%d').date()
                period_type = 'quarterly' # FMP 쿼터 데이터로 고정

                # None 값 처리 강화 (값이 없는 경우 'None' 문자열로 올 수도 있음)
                def get_numeric_value(data_dict, key):
                    value = data_dict.get(key)
                    if value is None or value == 'None' or value == '':
                        return None
                    try:
                        return float(value)
                    except ValueError:
                        logger.warning(f"[{symbol}] FMP 재무제표 필드 '{key}' 값 '{value}'를 숫자로 변환할 수 없습니다. None으로 처리합니다.")
                        return None

                revenue = get_numeric_value(statement, 'revenue')
                gross_profit = get_numeric_value(statement, 'grossProfit')
                operating_income = get_numeric_value(statement, 'operatingIncome')
                net_income = get_numeric_value(statement, 'netIncome')
                
                total_assets = get_numeric_value(statement, 'totalAssets')
                total_liabilities = get_numeric_value(statement, 'totalLiabilities')
                total_equity = get_numeric_value(statement, 'totalStockholdersEquity') # FMP 필드명 확인
                
                cash_from_operations = get_numeric_value(statement, 'cashFlowFromOperatingActivities') # FMP 필드명 확인

                # 1. 데이터베이스에 저장
                if conn and cur:
                    financial_records_for_db.append((
                        symbol, report_date, period_type, revenue, gross_profit,
                        operating_income, net_income, total_assets,
                        total_liabilities, total_equity, cash_from_operations
                    ))
                
                # CSV 저장을 위한 레코드 추가
                financial_records_for_csv.append({
                    'symbol': symbol,
                    'report_date': report_date,
                    'period': period_type,
                    'revenue': revenue,
                    'gross_profit': gross_profit,
                    'operating_income': operating_income,
                    'net_income': net_income,
                    'total_assets': total_assets,
                    'total_liabilities': total_liabilities,
                    'total_equity': total_equity,
                    'cash_from_operations': cash_from_operations
                })

            except ValueError as ve:
                logger.error(f"[{symbol}] 재무제표 {date_str}의 데이터 변환 오류: {ve}. 원본 데이터: {statement}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"[{symbol}] 재무제표 {date_str} 데이터 처리 오류: {e}", exc_info=True)
                continue
            
        # 데이터베이스 일괄 저장 (DB 커밋은 한 번만)
        if conn and cur and financial_records_for_db:
            try:
                db_insert_count = 0
                for record in financial_records_for_db:
                    cur.execute("""
                        INSERT INTO financials (
                            symbol, report_date, period, revenue, gross_profit,
                            operating_income, net_income, total_assets,
                            total_liabilities, total_equity, cash_from_operations
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, report_date, period) DO UPDATE
                        SET revenue = EXCLUDED.revenue, gross_profit = EXCLUDED.gross_profit,
                            operating_income = EXCLUDED.operating_income, net_income = EXCLUDED.net_income,
                            total_assets = EXCLUDED.total_assets, total_liabilities = EXCLUDED.total_liabilities,
                            total_equity = EXCLUDED.total_equity, cash_from_operations = EXCLUDED.cash_from_operations;
                    """, record)
                    db_insert_count += 1
                conn.commit()
                logger.info(f"[{symbol}] FMP 재무제표 데이터 {db_insert_count}개를 데이터베이스에 성공적으로 저장했습니다.")
            except Exception as e:
                logger.error(f"[{symbol}] 재무제표 데이터베이스 저장 중 오류: {e}", exc_info=True)
                conn.rollback()
        elif conn and cur:
            logger.info(f"[{symbol}] 수집된 재무제표 데이터가 없어 데이터베이스에 저장하지 않았습니다.")
        else:
            logger.warning(f"[{symbol}] 데이터베이스 연결 문제로 재무제표 데이터를 DB에 저장하지 않았습니다.")

        # 2. CSV 파일에 저장
        if financial_records_for_csv:
            df_financials = pd.DataFrame(financial_records_for_csv)
            df_financials['report_date'] = pd.to_datetime(df_financials['report_date'])
            df_financials = df_financials.sort_values(by='report_date').reset_index(drop=True)
            
            # (4) 파일 저장 경로 변경: FMP_DATA_FOLDER 사용
            file_path = os.path.join(FMP_DATA_FOLDER, f"{symbol}_financials.csv")
            ensure_data_folder_exists(FMP_DATA_FOLDER) # 상위 폴더가 확실히 존재하도록
            
            df_financials.to_csv(file_path, index=False)
            logger.info(f"[{symbol}] FMP 재무제표 데이터 {len(df_financials)}개를 '{file_path}'에 성공적으로 저장했습니다.")
        else:
            logger.warning(f"[{symbol}] 수집된 재무제표 데이터가 없어 CSV 파일로 저장하지 않았습니다.")

    except requests.exceptions.RequestException as e:
        logger.error(f"[{symbol}] FMP API 요청 오류: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[{symbol}] FMP 재무제표 데이터 수집 중 예상치 못한 오류: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Main Execution (이 부분은 main.py에서 호출되므로, collector 파일에서는 제거하거나 테스트용으로만 유지) ---
if __name__ == "__main__":
    logger.info("FMP_collector.py 스크립트 직접 실행 (테스트 목적).")

    # (5) 테스트 실행을 위한 폴더 생성 및 API 키 로드
    ensure_data_folder_exists(YFINANCE_DATA_FOLDER)
    ensure_data_folder_exists(FMP_DATA_FOLDER)

    fmp_api_key = config_loader.CONFIG['api_keys'].get('fmp')
    if not fmp_api_key:
        logger.critical("config.yaml에 'api_keys' 아래 'fmp' API 키가 설정되어 있지 않습니다. FMP 테스트 실행을 건너뜁니다.")
        exit(1)

    test_stock_symbols = config_loader.CONFIG['data_sources'].get('stocks', [])
    if not test_stock_symbols:
        logger.warning("config.yaml에 'data_sources' 아래 'stocks' 목록이 비어 있습니다. 테스트 실행을 건너뜁니다.")
        exit(1)

    today = datetime.now()
    one_year_ago = today - timedelta(days=365)
    
    # YFinance 주식 OHLCV 데이터 수집 테스트
    for symbol in test_stock_symbols[:1]: # 첫 번째 심볼만 테스트
        collect_and_save_stock_ohlcv_yfinance(symbol, one_year_ago.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d'))
        time.sleep(1) # 지연

    # FMP 재무제표 데이터 수집 테스트
    for symbol in test_stock_symbols[:1]: # 첫 번째 심볼만 테스트
        collect_and_save_financials_fmp(symbol, fmp_api_key)
        time.sleep(1) # 지연 (FMP API 호출 제한에 따라 더 길게 필요할 수 있음)

    logger.info("FMP_collector.py 테스트 실행 완료.")