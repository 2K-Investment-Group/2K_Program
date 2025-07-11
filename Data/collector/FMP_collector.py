import yfinance as yf
import ccxt
import requests
import psycopg2
from datetime import datetime, timedelta
import os 
import pandas as pd
import numpy as np
from Data.config.config_loader import CONFIG
import logging
from logger_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# --- 데이터 저장 경로 설정 ---
# 각 데이터 소스별로 별도의 폴더를 사용하도록 변경
DATA_FOLDER_YFINANCE = "Data_YahooFinance"
DATA_FOLDER_FMP = "Data_FMP"
DATA_FOLDER_CCXT = "Data_CCXT"

def ensure_data_folders_exist():
    """필요한 모든 데이터 폴더가 없으면 생성합니다."""
    for folder in [DATA_FOLDER_YFINANCE, DATA_FOLDER_FMP, DATA_FOLDER_CCXT]:
        if not os.path.exists(folder):
            os.makedirs(folder)
            logger.info(f"'{folder}' 폴더를 생성했습니다.")

# --- Database Connection ---
def get_db_connection():
    """데이터베이스 연결 객체를 반환합니다."""
    try:
        conn = psycopg2.connect(
            host=CONFIG['database']['host'],
            database=CONFIG['database']['dbname'],
            user=CONFIG['database']['user'],
            password=CONFIG['database']['password'],
            port=CONFIG['database']['port']
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        raise

# --- Stock OHLCV Data Collection ---
def collect_and_save_stock_ohlcv(symbol, start_date, end_date):
    logger.info(f"Collecting stock OHLCV for {symbol} from {start_date} to {end_date}...")
    
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
            logger.warning(f"No stock data found for {symbol} in the specified range. Skipping DB and CSV save.")
            return

        # CSV 파일 저장을 위한 데이터프레임 준비
        ohlcv_records_for_csv = []
        db_insert_count = 0
        
        for index, row in data.iterrows():
            trade_date = index.to_pydatetime().date() # 날짜만 필요하므로 .date() 추가
            
            # numpy NaN 값을 None으로 변환하여 데이터베이스와 CSV에 호환되도록 처리
            open_price = float(row['Open']) if pd.notna(row['Open']) else None
            high_price = float(row['High']) if pd.notna(row['High']) else None
            low_price = float(row['Low']) if pd.notna(row['Low']) else None
            close_price = float(row['Close']) if pd.notna(row['Close']) else None
            volume = int(row['Volume']) if pd.notna(row['Volume']) else None # Volume은 정수형으로

            # 1. 데이터베이스에 저장
            if conn and cur:
                try:
                    cur.execute("""
                        INSERT INTO stock_ohlcv (
                            symbol, timestamp, open, high, low, close, volume
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, timestamp) DO UPDATE SET 
                            open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                            close = EXCLUDED.close, volume = EXCLUDED.volume;
                    """, (symbol, trade_date, open_price, high_price, low_price, close_price, volume))
                    db_insert_count += 1
                except Exception as e:
                    logger.error(f"Error inserting/updating stock data for {symbol} at {trade_date} into DB: {e}", exc_info=True)
                    if conn: conn.rollback() # 오류 발생 시 롤백
            
            # CSV 저장을 위한 레코드 추가
            ohlcv_records_for_csv.append({
                'symbol': symbol,
                'timestamp': trade_date,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume
            })
        
        if conn and cur and db_insert_count > 0:
            conn.commit()
            logger.info(f"Successfully saved {db_insert_count} stock OHLCV records for {symbol} to database.")
        elif conn and cur:
             logger.info(f"No new or updated stock OHLCV records to save for {symbol} to database.")

        # 2. CSV 파일에 저장
        if ohlcv_records_for_csv:
            df_ohlcv = pd.DataFrame(ohlcv_records_for_csv)
            # timestamp 컬럼을 datetime으로 변환 후 정렬
            df_ohlcv['timestamp'] = pd.to_datetime(df_ohlcv['timestamp'])
            df_ohlcv = df_ohlcv.sort_values(by='timestamp').reset_index(drop=True)
            
            csv_file_path = os.path.join(DATA_FOLDER_YFINANCE, f"{symbol}_ohlcv.csv")
            df_ohlcv.to_csv(csv_file_path, index=False)
            logger.info(f"Successfully saved {len(df_ohlcv)} stock OHLCV records for {symbol} to '{csv_file_path}'.")
        else:
            logger.warning(f"No OHLCV data to save to CSV for {symbol}.")

    except Exception as e:
        logger.error(f"Error collecting or saving stock OHLCV for {symbol}: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Crypto OHLCV Data Collection ---
def collect_and_save_crypto_ohlcv(symbol, since_days=365, exchange_id='binance'):
    logger.info(f"Collecting crypto OHLCV for {symbol} from {exchange_id}...")
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class()

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception:
        logger.warning(f"[{symbol}] 암호화폐 OHLCV 데이터베이스 연결에 실패하여 DB 저장을 건너뜁니다. 파일 저장만 시도합니다.")

    try:
        since_timestamp_ms = exchange.parse8601((datetime.now() - timedelta(days=since_days)).isoformat())

        ohlcv = exchange.fetch_ohlcv(symbol, '1d', since=since_timestamp_ms)
        if not ohlcv:
            logger.warning(f"No crypto data found for {symbol} on {exchange_id}. Skipping DB and CSV save.")
            return

        crypto_records_for_csv = []
        db_insert_count = 0

        for candle in ohlcv:
            trade_date = datetime.fromtimestamp(candle[0] / 1000).date()
            open_price = float(candle[1]) if candle[1] is not None else None
            high_price = float(candle[2]) if candle[2] is not None else None
            low_price = float(candle[3]) if candle[3] is not None else None
            close_price = float(candle[4]) if candle[4] is not None else None
            volume = float(candle[5]) if candle[5] is not None else None

            # 1. 데이터베이스에 저장
            if conn and cur:
                try:
                    cur.execute("""
                        INSERT INTO crypto_ohlcv (
                            symbol, timestamp, open, high, low, close, volume
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, timestamp) DO UPDATE SET
                            open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                            close = EXCLUDED.close, volume = EXCLUDED.volume;
                    """, (symbol, trade_date, open_price, high_price, low_price, close_price, volume))
                    db_insert_count += 1
                except Exception as e:
                    logger.error(f"Error inserting/updating crypto data for {symbol} at {trade_date} into DB: {e}", exc_info=True)
                    if conn: conn.rollback()

            # CSV 저장을 위한 레코드 추가
            crypto_records_for_csv.append({
                'symbol': symbol,
                'timestamp': trade_date,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume
            })
        
        if conn and cur and db_insert_count > 0:
            conn.commit()
            logger.info(f"Successfully saved {db_insert_count} crypto OHLCV records for {symbol} to database.")
        elif conn and cur:
            logger.info(f"No new or updated crypto OHLCV records to save for {symbol} to database.")


        # 2. CSV 파일에 저장
        if crypto_records_for_csv:
            df_crypto_ohlcv = pd.DataFrame(crypto_records_for_csv)
            df_crypto_ohlcv['timestamp'] = pd.to_datetime(df_crypto_ohlcv['timestamp'])
            df_crypto_ohlcv = df_crypto_ohlcv.sort_values(by='timestamp').reset_index(drop=True)

            # 파일명에 '/'가 포함될 수 있으므로 교체 (예: BTC/USDT -> BTC_USDT)
            cleaned_symbol = symbol.replace('/', '_') 
            csv_file_path = os.path.join(DATA_FOLDER_CCXT, f"{cleaned_symbol}_crypto_ohlcv.csv")
            df_crypto_ohlcv.to_csv(csv_file_path, index=False)
            logger.info(f"Successfully saved {len(df_crypto_ohlcv)} crypto OHLCV records for {symbol} to '{csv_file_path}'.")
        else:
            logger.warning(f"No crypto OHLCV data to save to CSV for {symbol}.")

    except ccxt.NetworkError as e:
        logger.error(f"Network error while fetching crypto data for {symbol} from {exchange_id}: {e}")
    except ccxt.ExchangeError as e:
        logger.error(f"Exchange error while fetching crypto data for {symbol} from {exchange_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error collecting crypto OHLCV for {symbol}: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Financials Data Collection (Financial Modeling Prep Example) ---
def collect_and_save_financials(symbol, api_key_name_in_config):
    logger.info(f"Collecting financials for {symbol} using API key from '{api_key_name_in_config}'...")
    
    api_key = CONFIG['api_keys'].get(api_key_name_in_config.replace('_env', ''))

    if not api_key:
        logger.warning(f"API key for '{api_key_name_in_config}' not found or not set in environment variable. Skipping financials for {symbol}.")
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
        income_response = requests.get(url_income)
        balance_response = requests.get(url_balance)
        cashflow_response = requests.get(url_cashflow)

        income_data = []
        balance_data = []
        cashflow_data = []

        try:
            income_response.raise_for_status()
            income_data = income_response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error for income statement ({url_income}): {e} - Response: {income_response.text[:200]}...")
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"JSON Decode Error for income statement ({url_income}): {e} - Response: {income_response.text[:200]}...")

        try:
            balance_response.raise_for_status()
            balance_data = balance_response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error for balance sheet ({url_balance}): {e} - Response: {balance_response.text[:200]}...")
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"JSON Decode Error for balance sheet ({url_balance}): {e} - Response: {balance_response.text[:200]}...")

        try:
            cashflow_response.raise_for_status()
            cashflow_data = cashflow_response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error for cash flow ({url_cashflow}): {e} - Response: {cashflow_response.text[:200]}...")
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"JSON Decode Error for cash flow ({url_cashflow}): {e} - Response: {cashflow_response.text[:200]}...")

        combined_financials = {}
        # 먼저 income_data를 기준으로 데이터를 결합합니다.
        for item in income_data:
            # 'date' 필드가 없는 경우 'fillingDate'를 사용 (FMP API 응답에 따라)
            date_key = item.get('date') or item.get('fillingDate')
            if date_key:
                combined_financials[date_key] = {**item}

        # balance_data와 cashflow_data를 결합할 때, 이미 존재하는 date_key에 업데이트합니다.
        for item in balance_data:
            date_key = item.get('date') or item.get('fillingDate')
            if date_key:
                combined_financials.setdefault(date_key, {}).update(item)
        for item in cashflow_data:
            date_key = item.get('date') or item.get('fillingDate')
            if date_key:
                combined_financials.setdefault(date_key, {}).update(item)

        if not combined_financials:
            logger.warning(f"No financials found for {symbol} after combining. This might indicate API issues or no data.")
            return

        financial_records_for_csv = []
        db_insert_count = 0

        for date_str, statement in combined_financials.items():
            try:
                # '2023-09-30T16:30:00.000Z'와 같은 형식도 처리하기 위해 T 이후 부분을 자릅니다.
                report_date = datetime.strptime(date_str.split('T')[0], '%Y-%m-%d').date()
                period_type = 'quarterly' # FMP 쿼터 데이터로 고정

                # 데이터의 존재 여부를 명확히 확인하고 float으로 변환
                revenue = float(statement.get('revenue')) if statement.get('revenue') is not None else None
                gross_profit = float(statement.get('grossProfit')) if statement.get('grossProfit') is not None else None
                operating_income = float(statement.get('operatingIncome')) if statement.get('operatingIncome') is not None else None
                net_income = float(statement.get('netIncome')) if statement.get('netIncome') is not None else None
                
                # 대차대조표 항목 (FMP API 응답에 따라 키 이름 확인 필요)
                total_assets = float(statement.get('totalAssets')) if statement.get('totalAssets') is not None else None
                total_liabilities = float(statement.get('totalLiabilities')) if statement.get('totalLiabilities') is not None else None
                total_equity = float(statement.get('totalStockholdersEquity')) if statement.get('totalStockholdersEquity') is not None else None
                
                # 현금흐름표 항목 (FMP API 응답에 따라 키 이름 확인 필요)
                cash_from_operations = float(statement.get('cashFlowFromOperatingActivities')) if statement.get('cashFlowFromOperatingActivities') is not None else None

                # 1. 데이터베이스에 저장
                if conn and cur:
                    try:
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
                        """, (
                            symbol, report_date, period_type, revenue, gross_profit,
                            operating_income, net_income, total_assets,
                            total_liabilities, total_equity, cash_from_operations
                        ))
                        db_insert_count += 1
                    except Exception as e:
                        logger.error(f"Error inserting/updating financial data for {symbol} on {report_date} ({period_type}) into DB: {e}", exc_info=True)
                        if conn: conn.rollback()
                        continue # 이 레코드의 DB 저장은 실패했지만 다음 레코드 처리
                
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
                logger.error(f"Data conversion error for financials of {symbol} on {date_str}: {ve}. Raw data: {statement}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"Error processing financial data for {symbol} on {date_str}: {e}", exc_info=True)
                continue
        
        if conn and cur and db_insert_count > 0:
            conn.commit()
            logger.info(f"Successfully saved {db_insert_count} financial records for {symbol} to database.")
        elif conn and cur:
            logger.info(f"No new or updated financial records to save for {symbol} to database.")

        # 2. CSV 파일에 저장
        if financial_records_for_csv:
            df_financials = pd.DataFrame(financial_records_for_csv)
            df_financials['report_date'] = pd.to_datetime(df_financials['report_date'])
            df_financials = df_financials.sort_values(by='report_date').reset_index(drop=True)
            
            csv_file_path = os.path.join(DATA_FOLDER_FMP, f"{symbol}_financials.csv")
            df_financials.to_csv(csv_file_path, index=False)
            logger.info(f"Successfully saved {len(df_financials)} financial records for {symbol} to '{csv_file_path}'.")
        else:
            logger.warning(f"No financial data to save to CSV for {symbol}.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Network or API connectivity error for {symbol}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error collecting financials for {symbol}: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Main Execution ---
if __name__ == "__main__":
    logger.info("YFinance & Crypto & FMP 데이터 수집 스크립트 시작 (DB 및 파일 동시 저장 모드).")

    ensure_data_folders_exist() # 폴더 생성 함수 호출

    today = datetime.now()
    one_year_ago = today - timedelta(days=365)
    
    # 주식 OHLCV 데이터 수집 (YFinance)
    stock_symbols_yf = CONFIG['data_sources'].get('stocks', [])
    if not stock_symbols_yf:
        logger.warning("config.yaml에 'data_sources' 아래 'stocks' 목록이 비어 있습니다. YFinance 주식 데이터 수집을 건너뜁니다.")
    
    logger.info("YFinance 주식 OHLCV 데이터 수집 시작.")
    for stock_symbol in stock_symbols_yf:
        collect_and_save_stock_ohlcv(stock_symbol, one_year_ago.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d'))
    logger.info("모든 YFinance 주식 OHLCV 데이터 수집 완료.")

    # 암호화폐 OHLCV 데이터 수집 (CCXT)
    crypto_symbols = CONFIG['data_sources'].get('cryptos', [])
    if not crypto_symbols:
        logger.warning("config.yaml에 'data_sources' 아래 'cryptos' 목록이 비어 있습니다. 암호화폐 데이터 수집을 건너뜨니다.")

    logger.info("CCXT 암호화폐 OHLCV 데이터 수집 시작.")
    for crypto_symbol in crypto_symbols:
        collect_and_save_crypto_ohlcv(crypto_symbol, since_days=365)
    logger.info("모든 CCXT 암호화폐 데이터 수집 완료.")

    # 재무제표 데이터 수집 (Financial Modeling Prep)
    # config.yaml의 financial_modeling_prep_env -> config_loader에서 financial_modeling_prep으로 변경됨
    fmp_api_key_name = 'financial_modeling_prep' 
    if not CONFIG['api_keys'].get(fmp_api_key_name):
        logger.warning(f"config.yaml에 'api_keys' 아래 '{fmp_api_key_name}' API 키가 설정되어 있지 않습니다. 재무제표 수집을 건너뜨니다.")
    
    logger.info("FMP 재무제표 데이터 수집 시작.")
    for stock_symbol in stock_symbols_yf: # 주식 심볼을 재사용
        collect_and_save_financials(stock_symbol, fmp_api_key_name)
    logger.info("모든 FMP 재무제표 데이터 수집 완료.")

    logger.info("전체 데이터 수집 스크립트 종료 (DB 및 파일 동시 저장 모드).")