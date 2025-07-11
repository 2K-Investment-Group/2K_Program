import requests
import psycopg2 
from datetime import datetime, timedelta
import os
import pandas as pd 
import time 
import logging

from config_loader import CONFIG 
from logger_config import setup_logging 

setup_logging()
logger = logging.getLogger(__name__)

# --- 데이터 저장 기본 경로 설정 ---
DATA_FOLDER = "Data_AlphaVantage"

def ensure_data_folder_exists():
    """기본 데이터 폴더가 없으면 생성합니다."""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
        logger.info(f"'{DATA_FOLDER}' 폴더를 생성했습니다.")

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
        logger.error(f"데이터베이스 연결 오류: {e}")
        raise # 연결 실패 시 예외 발생

# --- Stock OHLCV Data Collection (AlphaVantage 기반) ---
def collect_and_save_stock_ohlcv_alphavantage(symbol, api_key, outputsize='full'):
    """
    AlphaVantage API를 사용하여 특정 주식의 일별 OHLCV 데이터를 수집하고
    데이터베이스와 CSV 파일에 모두 저장합니다. CSV 파일은 거래소/산업분야 폴더에 저장됩니다.
    """
    logger.info(f"[{symbol}] AlphaVantage에서 주식 OHLCV 데이터 수집 시작...")
    
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize={outputsize}&apikey={api_key}"
    
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception:
        logger.error(f"[{symbol}] OHLCV 데이터베이스 연결에 실패하여 DB 저장 작업을 건너뜁니다. 파일 저장만 시도합니다.")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Error Message" in data:
            logger.error(f"[{symbol}] AlphaVantage API 오류: {data['Error Message']}")
            return
        if "Note" in data:
            logger.warning(f"[{symbol}] AlphaVantage API 노트: {data['Note']}")
            return
        if "Time Series (Daily)" not in data:
            logger.warning(f"[{symbol}] AlphaVantage에서 주식 데이터를 찾을 수 없습니다. 응답: {data}")
            return

        time_series = data["Time Series (Daily)"]
        
        ohlcv_db_records = [] 
        ohlcv_file_records = [] 

        for date_str, values in time_series.items():
            try:
                trade_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                open_price = float(values['1. open']) if values['1. open'] != 'None' else None
                high_price = float(values['2. high']) if values['2. high'] != 'None' else None
                low_price = float(values['3. low']) if values['3. low'] != 'None' else None
                close_price = float(values['4. close']) if values['4. close'] != 'None' else None
                volume = int(values['5. volume']) if values['5. volume'] != 'None' else None

                ohlcv_db_records.append((
                    symbol, trade_date, open_price, high_price, low_price, close_price, volume
                ))
                ohlcv_file_records.append({
                    'symbol': symbol, 'timestamp': trade_date, 'open': open_price,
                    'high': high_price, 'low': low_price, 'close': close_price, 'volume': volume
                })
            except (ValueError, TypeError) as ve:
                logger.error(f"[{symbol}] 날짜 {date_str}의 데이터 변환 오류: {ve}. 데이터: {values}")
                continue
            except Exception as e:
                logger.error(f"[{symbol}] 날짜 {date_str}의 주식 데이터 처리 오류: {e}", exc_info=True)
                continue
        
        # --- 1. 데이터베이스에 저장 ---
        if conn and cur:
            if ohlcv_db_records:
                try:
                    db_insert_count = 0
                    for record in ohlcv_db_records:
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
                    logger.info(f"[{symbol}] AlphaVantage OHLCV 데이터 {db_insert_count}개를 데이터베이스에 성공적으로 저장했습니다.")
                except Exception as e:
                    logger.error(f"[{symbol}] OHLCV 데이터베이스 저장 중 오류: {e}", exc_info=True)
                    conn.rollback() 
            else:
                logger.warning(f"[{symbol}] 수집된 OHLCV 데이터가 없어 데이터베이스에 저장하지 않았습니다.")
        else:
            logger.warning(f"[{symbol}] 데이터베이스 연결 문제로 OHLCV 데이터를 DB에 저장하지 않았습니다.")

        # --- 2. CSV 파일에 저장 (거래소/산업분야 폴더 구조) ---
        if ohlcv_file_records:
            df = pd.DataFrame(ohlcv_file_records)
            df['timestamp'] = pd.to_datetime(df['timestamp']) 
            df = df.sort_values(by='timestamp').reset_index(drop=True) 
            
            # 주식 정보 조회 (거래소, 산업 분야)
            stock_info = get_stock_info_from_db(symbol)
            
            exchange_name = stock_info.get('exchange', 'Unknown_Exchange').replace('/', '_').replace('\\', '_') if stock_info else 'Unknown_Exchange'
            industry_name = stock_info.get('industry', 'Unknown_Industry').replace('/', '_').replace('\\', '_') if stock_info else 'Unknown_Industry'
            
            # 폴더 경로 생성
            target_folder = os.path.join(DATA_FOLDER, exchange_name, industry_name)
            os.makedirs(target_folder, exist_ok=True) # 존재하지 않으면 폴더 생성
            
            file_path = os.path.join(target_folder, f"{symbol}_ohlcv.csv")
            df.to_csv(file_path, index=False) 
            logger.info(f"[{symbol}] AlphaVantage OHLCV 데이터 {len(df)}개를 '{file_path}'에 성공적으로 저장했습니다.")
        else:
            logger.warning(f"[{symbol}] 수집된 OHLCV 데이터가 없어 CSV 파일로 저장하지 않았습니다.")

    except requests.exceptions.RequestException as e:
        logger.error(f"[{symbol}] AlphaVantage OHLCV API 요청 오류: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[{symbol}] AlphaVantage OHLCV 데이터 수집 중 예상치 못한 오류: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            
# --- Financials Data Collection (AlphaVantage 기반) ---
def collect_and_save_financials_alphavantage(symbol, api_key):
    """
    AlphaVantage API를 사용하여 특정 주식의 재무제표 데이터를 수집하고
    데이터베이스와 CSV 파일에 모두 저장합니다. CSV 파일은 거래소/산업분야 폴더에 저장됩니다.
    """
    logger.info(f"[{symbol}] AlphaVantage에서 재무제표 데이터 수집 시작...")
    
    if not api_key:
        logger.warning(f"[{symbol}] AlphaVantage API 키를 찾을 수 없습니다. 재무제표 수집을 건너뜁니다.")
        return

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception:
        logger.error(f"[{symbol}] 재무제표 데이터베이스 연결에 실패하여 DB 저장 작업을 건너뜁니다. 파일 저장만 시도합니다.")

    try:
        urls = {
            'income': f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey={api_key}",
            'balance': f"https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={symbol}&apikey={api_key}",
            'cashflow': f"https://www.alphavantage.co/query?function=CASH_FLOW&symbol={symbol}&apikey={api_key}"
        }

        financial_statements_raw = {}
        for stmt_type, url in urls.items():
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if "Error Message" in data:
                    logger.error(f"[{symbol}] AlphaVantage {stmt_type} API 오류: {data['Error Message']}")
                    financial_statements_raw[stmt_type] = []
                    continue
                if "Note" in data:
                    logger.warning(f"[{symbol}] AlphaVantage {stmt_type} API 노트: {data['Note']}")
                    financial_statements_raw[stmt_type] = []
                    continue
                
                quarterly_reports = data.get('quarterlyReports', [])
                annual_reports = data.get('annualReports', [])

                if quarterly_reports:
                    financial_statements_raw[stmt_type] = [{'report': r, 'period_type': 'quarterly'} for r in quarterly_reports]
                elif annual_reports:
                    financial_statements_raw[stmt_type] = [{'report': r, 'period_type': 'annual'} for r in annual_reports]
                else:
                    logger.warning(f"[{symbol}] AlphaVantage {stmt_type}에서 분기/연간 보고서를 찾을 수 없습니다. 응답: {data}")
                    financial_statements_raw[stmt_type] = []

            except requests.exceptions.RequestException as e:
                logger.error(f"[{symbol}] AlphaVantage {stmt_type} API 요청 오류: {e}", exc_info=True)
                financial_statements_raw[stmt_type] = []
            except Exception as e:
                logger.error(f"[{symbol}] AlphaVantage {stmt_type} 데이터 파싱 중 예상치 못한 오류: {e}", exc_info=True)
                financial_statements_raw[stmt_type] = []
            finally:
                time.sleep(15) # 각 API 호출 사이에 15초 지연

        combined_financials = {}
        for stmt_type, reports_list in financial_statements_raw.items():
            for entry in reports_list:
                report = entry['report']
                period_type = entry['period_type']
                date_key = report.get('fiscalDateEnding')
                if date_key:
                    if date_key not in combined_financials:
                        combined_financials[date_key] = {'period': period_type, 'symbol': symbol, 'report_date': date_key}
                    combined_financials[date_key].update(report)

        if not combined_financials:
            logger.warning(f"[{symbol}] AlphaVantage에서 통합된 재무제표 데이터를 찾을 수 없습니다.")
            return

        financial_db_records = []
        financial_file_records = []
        for date_str, statement in combined_financials.items():
            try:
                report_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                period_type = statement.get('period', 'unknown') 

                revenue = float(statement.get('totalRevenue')) if statement.get('totalRevenue') not in [None, 'None'] else None
                gross_profit = float(statement.get('grossProfit')) if statement.get('grossProfit') not in [None, 'None'] else None
                operating_income = float(statement.get('operatingIncome')) if statement.get('operatingIncome') not in [None, 'None'] else None
                net_income = float(statement.get('netIncome')) if statement.get('netIncome') not in [None, 'None'] else None
                
                total_assets = float(statement.get('totalAssets')) if statement.get('totalAssets') not in [None, 'None'] else None
                total_liabilities = float(statement.get('totalLiabilities')) if statement.get('totalLiabilities') not in [None, 'None'] else None
                total_equity = float(statement.get('totalShareholderEquity')) if statement.get('totalShareholderEquity') not in [None, 'None'] else None
                
                cash_from_operations = float(statement.get('operatingCashflow')) if statement.get('operatingCashflow') not in [None, 'None'] else None

                financial_db_records.append((
                    symbol, report_date, period_type, revenue, gross_profit,
                    operating_income, net_income, total_assets,
                    total_liabilities, total_equity, cash_from_operations
                ))
                financial_file_records.append({
                    'symbol': symbol, 'report_date': report_date, 'period': period_type,
                    'revenue': revenue, 'gross_profit': gross_profit, 'operating_income': operating_income,
                    'net_income': net_income, 'total_assets': total_assets, 'total_liabilities': total_liabilities,
                    'total_equity': total_equity, 'cash_from_operations': cash_from_operations
                })
            except (ValueError, TypeError) as ve:
                logger.error(f"[{symbol}] 재무제표 {date_str}의 데이터 변환 오류: {ve}. 데이터: {statement}")
                continue
            except Exception as e:
                logger.error(f"[{symbol}] 재무제표 {date_str} 데이터 처리 오류: {e}", exc_info=True)
                continue
            
        # --- 1. 데이터베이스에 저장 ---
        if conn and cur:
            if financial_db_records:
                try:
                    db_insert_count = 0
                    for record in financial_db_records:
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
                    logger.info(f"[{symbol}] AlphaVantage 재무제표 데이터 {db_insert_count}개를 데이터베이스에 성공적으로 저장했습니다.")
                except Exception as e:
                    logger.error(f"[{symbol}] 재무제표 데이터베이스 저장 중 오류: {e}", exc_info=True)
                    conn.rollback()
            else:
                logger.warning(f"[{symbol}] 수집된 재무제표 데이터가 없어 데이터베이스에 저장하지 않았습니다.")
        else:
            logger.warning(f"[{symbol}] 데이터베이스 연결 문제로 재무제표 데이터를 DB에 저장하지 않았습니다.")

        # --- 2. CSV 파일에 저장 (거래소/산업분야 폴더 구조) ---
        if financial_file_records:
            df = pd.DataFrame(financial_file_records)
            df['report_date'] = pd.to_datetime(df['report_date'])
            df = df.sort_values(by='report_date').reset_index(drop=True)
            
            # 주식 정보 조회 (거래소, 산업 분야)
            stock_info = get_stock_info_from_db(symbol)
            
            exchange_name = stock_info.get('exchange', 'Unknown_Exchange').replace('/', '_').replace('\\', '_') if stock_info else 'Unknown_Exchange'
            industry_name = stock_info.get('industry', 'Unknown_Industry').replace('/', '_').replace('\\', '_') if stock_info else 'Unknown_Industry'
            
            # 폴더 경로 생성
            target_folder = os.path.join(DATA_FOLDER, exchange_name, industry_name)
            os.makedirs(target_folder, exist_ok=True) # 존재하지 않으면 폴더 생성

            file_path = os.path.join(target_folder, f"{symbol}_financials.csv")
            df.to_csv(file_path, index=False)
            logger.info(f"[{symbol}] AlphaVantage 재무제표 데이터 {len(df)}개를 '{file_path}'에 성공적으로 저장했습니다.")
        else:
            logger.warning(f"[{symbol}] 수집된 재무제표 데이터가 없어 CSV 파일로 저장하지 않았습니다.")
    
    except requests.exceptions.RequestException as e:
        logger.error(f"[{symbol}] AlphaVantage 재무제표 API 요청 오류: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[{symbol}] 재무제표 데이터 수집 중 예상치 못한 오류: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Stock Company Overview Data Collection (AlphaVantage 기반) ---
def collect_stock_info_alphavantage(symbol, api_key):
    """
    AlphaVantage API를 사용하여 특정 주식의 회사 개요(거래소, 산업 분야 등)를 수집하고
    데이터베이스의 stock_info 테이블에 저장합니다.
    """
    logger.info(f"[{symbol}] AlphaVantage에서 회사 개요 데이터 수집 시작...")

    if not api_key:
        logger.warning(f"[{symbol}] AlphaVantage API 키를 찾을 수 없습니다. 회사 개요 수집을 건너뜁니다.")
        return {}

    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={api_key}"

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception:
        logger.error(f"[{symbol}] 회사 개요 데이터베이스 연결에 실패하여 DB 저장 작업을 건너뜁니다. API 응답만 반환합니다.")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Error Message" in data:
            logger.error(f"[{symbol}] AlphaVantage Company Overview API 오류: {data['Error Message']}")
            return {}
        if not data or len(data) == 0:
            logger.warning(f"[{symbol}] AlphaVantage에서 회사 개요 데이터를 찾을 수 없습니다. 응답: {data}")
            return {}

        # 데이터베이스에 저장
        if conn and cur:
            try:
                cur.execute("""
                    INSERT INTO stock_info (
                        symbol, asset_type, name, description, exchange, currency, country,
                        sector, industry, market_capitalization, pe_ratio, dividend_yield
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol) DO UPDATE SET 
                        asset_type = EXCLUDED.asset_type,
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        exchange = EXCLUDED.exchange,
                        currency = EXCLUDED.currency,
                        country = EXCLUDED.country,
                        sector = EXCLUDED.sector,
                        industry = EXCLUDED.industry,
                        market_capitalization = EXCLUDED.market_capitalization,
                        pe_ratio = EXCLUDED.pe_ratio,
                        dividend_yield = EXCLUDED.dividend_yield,
                        updated_at = CURRENT_TIMESTAMP;
                """, (
                    symbol,
                    data.get('AssetType'),
                    data.get('Name'),
                    data.get('Description'),
                    data.get('Exchange'),
                    data.get('Currency'),
                    data.get('Country'),
                    data.get('Sector'),
                    data.get('Industry'),
                    int(data['MarketCapitalization']) if data.get('MarketCapitalization') not in [None, 'None'] else None,
                    float(data['PERatio']) if data.get('PERatio') not in [None, 'None'] else None,
                    float(data['DividendYield']) if data.get('DividendYield') not in [None, 'None'] else None
                ))
                conn.commit()
                logger.info(f"[{symbol}] 회사 개요 데이터를 데이터베이스에 성공적으로 저장/업데이트했습니다.")
            except Exception as e:
                logger.error(f"[{symbol}] 회사 개요 데이터베이스 저장 중 오류: {e}", exc_info=True)
                conn.rollback()
        else:
            logger.warning(f"[{symbol}] 데이터베이스 연결 문제로 회사 개요 데이터를 DB에 저장하지 않았습니다.")

        return {
            'symbol': symbol,
            'name': data.get('Name'),
            'exchange': data.get('Exchange'),
            'sector': data.get('Sector'),
            'industry': data.get('Industry')
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"[{symbol}] AlphaVantage Company Overview API 요청 오류: {e}", exc_info=True)
        return {}
    except Exception as e:
        logger.error(f"[{symbol}] 회사 개요 데이터 수집 중 예상치 못한 오류: {e}", exc_info=True)
        return {}
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_stock_info_from_db(symbol):
    """
    데이터베이스에서 특정 주식의 거래소와 산업 분야 정보를 조회합니다.
    """
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT exchange, industry FROM stock_info WHERE symbol = %s;", (symbol,))
        result = cur.fetchone()
        if result:
            return {'exchange': result[0], 'industry': result[1]}
        return {'exchange': 'Unknown_Exchange', 'industry': 'Unknown_Industry'} # 정보 없을 시 기본값 반환
    except Exception as e:
        logger.error(f"[{symbol}] 데이터베이스에서 주식 정보를 가져오는 중 오류 발생: {e}", exc_info=True)
        return {'exchange': 'Error_Exchange', 'industry': 'Error_Industry'} # 오류 시 기본값 반환
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# --- Crypto OHLCV Data Collection (ccxt 기반 - 기존 코드 유지) ---
import ccxt 

def collect_and_save_crypto_ohlcv(symbol, since_days=365, exchange_id='binance'):
    """
    ccxt 라이브러리를 사용하여 암호화폐 OHLCV 데이터를 수집하고
    데이터베이스와 CSV 파일에 모두 저장합니다.
    """
    logger.info(f"[{symbol}] {exchange_id}에서 암호화폐 OHLCV 데이터 수집 시작...")
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class()

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception:
        logger.error(f"[{symbol}] 암호화폐 데이터베이스 연결에 실패하여 DB 저장 작업을 건너킵니다. 파일 저장만 시도합니다.")

    try:
        since_timestamp_ms = exchange.parse8601((datetime.now() - timedelta(days=since_days)).isoformat())

        ohlcv = exchange.fetch_ohlcv(symbol, '1d', since=since_timestamp_ms)
        if not ohlcv:
            logger.warning(f"[{symbol}] {exchange_id}에서 암호화폐 데이터를 찾을 수 없습니다.")
            return

        crypto_db_records = []
        crypto_file_records = []
        for candle in ohlcv:
            trade_date = datetime.fromtimestamp(candle[0] / 1000).date()
            open_price = float(candle[1]) if candle[1] is not None else None
            high_price = float(candle[2]) if candle[2] is not None else None
            low_price = float(candle[3]) if candle[3] is not None else None
            close_price = float(candle[4]) if candle[4] is not None else None
            volume = float(candle[5]) if candle[5] is not None else None

            crypto_db_records.append((
                symbol, trade_date, open_price, high_price, low_price, close_price, volume
            ))
            crypto_file_records.append({
                'symbol': symbol, 'timestamp': trade_date, 'open': open_price,
                'high': high_price, 'low': low_price, 'close': close_price, 'volume': volume
            })
        
        # --- 1. 데이터베이스에 저장 ---
        if conn and cur:
            if crypto_db_records:
                try:
                    db_insert_count = 0
                    for record in crypto_db_records:
                        cur.execute("""
                            INSERT INTO crypto_ohlcv (
                                symbol, timestamp, open, high, low, close, volume
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                                open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                                close = EXCLUDED.close, volume = EXCLUDED.volume;
                        """, record)
                        db_insert_count += 1
                    conn.commit()
                    logger.info(f"[{symbol}] {exchange_id}에서 암호화폐 OHLCV 데이터 {db_insert_count}개를 데이터베이스에 성공적으로 저장했습니다.")
                except Exception as e:
                    logger.error(f"[{symbol}] 암호화폐 OHLCV 데이터베이스 저장 중 오류: {e}", exc_info=True)
                    conn.rollback()
            else:
                logger.warning(f"[{symbol}] 수집된 암호화폐 OHLCV 데이터가 없어 데이터베이스에 저장하지 않았습니다.")
        else:
            logger.warning(f"[{symbol}] 데이터베이스 연결 문제로 암호화폐 OHLCV 데이터를 DB에 저장하지 않았습니다.")

        # --- 2. CSV 파일에 저장 ---
        if crypto_file_records:
            df = pd.DataFrame(crypto_file_records)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values(by='timestamp').reset_index(drop=True)
            
            # 암호화폐는 거래소/산업분야 폴더 구조를 따르지 않음
            file_path = os.path.join(DATA_FOLDER, f"{symbol.replace('/', '_')}_crypto_ohlcv.csv")
            df.to_csv(file_path, index=False)
            logger.info(f"[{symbol}] {exchange_id}에서 암호화폐 OHLCV 데이터 {len(df)}개를 '{file_path}'에 성공적으로 저장했습니다.")
        else:
            logger.warning(f"[{symbol}] 수집된 암호화폐 OHLCV 데이터가 없어 CSV 파일로 저장하지 않았습니다.")

    except ccxt.NetworkError as e:
        logger.error(f"[{symbol}] {exchange_id}에서 암호화폐 데이터 패치 중 네트워크 오류: {e}")
    except ccxt.ExchangeError as e:
        logger.error(f"[{symbol}] {exchange_id}에서 암호화폐 데이터 패치 중 거래소 오류: {e}")
    except Exception as e:
        logger.error(f"[{symbol}] 암호화폐 OHLCV 데이터 수집/저장 중 예상치 못한 오류: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Main Execution Block ---
if __name__ == "__main__":
    logger.info("AlphaVantage & Crypto 데이터 수집 스크립트 시작 (DB 및 파일 동시 저장 모드).")

    ensure_data_folder_exists()

    alphavantage_api_key = CONFIG['api_keys'].get('alphavantage') 
    if not alphavantage_api_key:
        logger.critical("config.yaml에 'api_keys' 아래 'alphavantage' API 키가 설정되어 있지 않습니다. 스크립트를 종료합니다.")
        exit(1) 

    stock_symbols = CONFIG['data_sources'].get('stocks', [])
    if not stock_symbols:
        logger.warning("config.yaml에 'data_sources' 아래 'stocks' 목록이 비어 있습니다. 주식 데이터 수집을 건너킵니다.")

    # --- 1. 주식 회사 개요(거래소, 산업 분야) 데이터 수집 ---
    # 폴더 구조 생성을 위해 OHLCV/재무 데이터 수집 전에 반드시 실행되어야 합니다.
    logger.info("주식 회사 개요 데이터 수집 시작.")
    for symbol in stock_symbols:
        collect_stock_info_alphavantage(symbol, alphavantage_api_key)
        logger.info(f"API 호출 제한을 위해 {symbol} 회사 개요 처리 후 15초 대기 중...")
        time.sleep(15)
    logger.info("모든 주식 회사 개요 데이터 수집 완료.")

    # --- 2. 주식 OHLCV 데이터 수집 ---
    logger.info("주식 OHLCV 데이터 수집 시작.")
    for symbol in stock_symbols:
        collect_and_save_stock_ohlcv_alphavantage(symbol, alphavantage_api_key)
        logger.info(f"API 호출 제한을 위해 {symbol} OHLCV 처리 후 15초 대기 중...")
        time.sleep(15) 
    logger.info("모든 주식 OHLCV 데이터 수집 완료.")
    
    # --- 3. 주식 재무제표 데이터 수집 ---
    logger.info("주식 재무제표 데이터 수집 시작.")
    for symbol in stock_symbols:
        collect_and_save_financials_alphavantage(symbol, alphavantage_api_key)
        logger.info(f"API 호출 제한을 위해 {symbol} 재무제표 처리 후 15초 대기 중...")
        time.sleep(15) 
    logger.info("모든 주식 재무제표 데이터 수집 완료.")
    
    crypto_symbols = CONFIG['data_sources'].get('cryptos', [])
    if not crypto_symbols:
        logger.warning("config.yaml에 'data_sources' 아래 'cryptos' 목록이 비어 있습니다. 암호화폐 데이터 수집을 건너킵니다.")

    logger.info("암호화폐 OHLCV 데이터 수집 시작.")
    for crypto_symbol in crypto_symbols:
        # 암호화폐는 거래소/산업분야 폴더 구조를 적용하지 않습니다.
        collect_and_save_crypto_ohlcv(crypto_symbol, since_days=365)

    logger.info("모든 암호화폐 데이터 수집 완료.")

    logger.info("데이터 수집 스크립트 종료 (DB 및 파일 동시 저장 모드).")