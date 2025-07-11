import yfinance as yf
import requests
import psycopg2
from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
import logging
import time
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from Data.config import config_loader  
from utils.logger_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
RAW_DATA_ROOT = os.path.join(BASE_DIR, "data", "raw_data")

YFINANCE_DATA_FOLDER = os.path.join(RAW_DATA_ROOT, "yfinance")

FMP_DATA_FOLDER = os.path.join(RAW_DATA_ROOT, "fmp")

def ensure_data_folder_exists(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        logger.info(f"Created folder: '{folder_path}'.")

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=config_loader.CONFIG['database']['host'],
            database=config_loader.CONFIG['database']['dbname'],
            user=config_loader.CONFIG['database']['user'],
            password=config_loader.CONFIG['database']['password'],
            port=config_loader.CONFIG['database']['port']
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

def collect_and_save_stock_ohlcv_yfinance(symbol, start_date, end_date):
    logger.info(f"[{symbol}] Starting collection of stock OHLCV data from YFinance (Period: {start_date} ~ {end_date})...")
    
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception:
        logger.warning(f"[{symbol}] Failed to connect to the database for stock OHLCV data. Skipping DB save. Attempting file save only.")

    try:
        data = yf.download(symbol, start=start_date, end=end_date)
        if data.empty:
            logger.warning(f"[{symbol}] No stock data found from YFinance for the specified period. Skipping DB and CSV save.")
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
                    logger.info(f"[{symbol}] Successfully saved {db_insert_count} YFinance OHLCV data points to the database.")
                except Exception as e:
                    logger.error(f"[{symbol}] Error saving OHLCV data to database: {e}", exc_info=True)
                    conn.rollback() 
            else:
                logger.warning(f"[{symbol}] No OHLCV data collected, so not saving to database.")
        else:
            logger.warning(f"[{symbol}] Due to database connection issues, OHLCV data was not saved to DB.")

        if ohlcv_records_for_csv:
            df_ohlcv = pd.DataFrame(ohlcv_records_for_csv)
            df_ohlcv['timestamp'] = pd.to_datetime(df_ohlcv['timestamp']) 
            df_ohlcv = df_ohlcv.sort_values(by='timestamp').reset_index(drop=True) 

            file_path = os.path.join(YFINANCE_DATA_FOLDER, f"{symbol}_ohlcv.csv")
            ensure_data_folder_exists(YFINANCE_DATA_FOLDER) 
            
            df_ohlcv.to_csv(file_path, index=False) 
            logger.info(f"[{symbol}] Successfully saved {len(df_ohlcv)} YFinance OHLCV data points to '{file_path}'.")
        else:
            logger.warning(f"[{symbol}] No OHLCV data collected, so not saving to CSV file.")

    except Exception as e:
        logger.error(f"[{symbol}] Unexpected error during YFinance OHLCV data collection/save: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def collect_and_save_financials_fmp(symbol, api_key): 
    logger.info(f"[{symbol}] Starting collection of financial statement data from FMP...")
    
    if not api_key:
        logger.warning(f"[{symbol}] FMP API key not found. Skipping financial statement collection.")
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
        logger.warning(f"[{symbol}] Failed to connect to the database for financial statement data. Skipping DB save. Attempting file save only.")

    try:
        income_data = []
        balance_data = []
        cashflow_data = []

        def fetch_json(url, statement_type):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()
                if not data:
                    logger.warning(f"[{symbol}] FMP {statement_type} data is empty. URL: {url}")
                return data
            except requests.exceptions.RequestException as e:
                logger.error(f"[{symbol}] FMP {statement_type} API request error ({url}): {e}")
                return []
            except requests.exceptions.JSONDecodeError as e:
                logger.error(f"[{symbol}] FMP {statement_type} JSON parsing error ({url}): {e} - Response: {response.text[:200]}...")
                return []
            except Exception as e:
                logger.error(f"[{symbol}] Unexpected error fetching FMP {statement_type} data ({url}): {e}", exc_info=True)
                return []

        income_data = fetch_json(url_income, 'income statement')
        time.sleep(1) 
        balance_data = fetch_json(url_balance, 'balance sheet')
        time.sleep(1)
        cashflow_data = fetch_json(url_cashflow, 'cash flow statement')

        combined_financials = {}
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
            logger.warning(f"[{symbol}] No consolidated financial statement data found from FMP. API issue or no data.")
            return

        financial_records_for_db = []
        financial_records_for_csv = []

        for date_str, statement in combined_financials.items():
            try:
                report_date = datetime.strptime(date_str.split('T')[0], '%Y-%m-%d').date()
                period_type = 'quarterly'

                def get_numeric_value(data_dict, key):
                    value = data_dict.get(key)
                    if value is None or value == 'None' or value == '':
                        return None
                    try:
                        return float(value)
                    except ValueError:
                        logger.warning(f"[{symbol}] Could not convert FMP financial field '{key}' value '{value}' to a number. Treating as None.")
                        return None

                revenue = get_numeric_value(statement, 'revenue')
                gross_profit = get_numeric_value(statement, 'grossProfit')
                operating_income = get_numeric_value(statement, 'operatingIncome')
                net_income = get_numeric_value(statement, 'netIncome')
                
                total_assets = get_numeric_value(statement, 'totalAssets')
                total_liabilities = get_numeric_value(statement, 'totalLiabilities')
                total_equity = get_numeric_value(statement, 'totalStockholdersEquity') 
                
                cash_from_operations = get_numeric_value(statement, 'cashFlowFromOperatingActivities') 

                if conn and cur:
                    financial_records_for_db.append((
                        symbol, report_date, period_type, revenue, gross_profit,
                        operating_income, net_income, total_assets,
                        total_liabilities, total_equity, cash_from_operations
                    ))

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
                logger.error(f"[{symbol}] Data conversion error for financial statement {date_str}: {ve}. Original data: {statement}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"[{symbol}] Error processing financial statement data for {date_str}: {e}", exc_info=True)
                continue

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
                logger.info(f"[{symbol}] Successfully saved {db_insert_count} FMP financial statement data points to the database.")
            except Exception as e:
                logger.error(f"[{symbol}] Error saving financial statement data to database: {e}", exc_info=True)
                conn.rollback()
        elif conn and cur:
            logger.info(f"[{symbol}] No financial statement data collected, so not saving to database.")
        else:
            logger.warning(f"[{symbol}] Due to database connection issues, financial statement data was not saved to DB.")

        if financial_records_for_csv:
            df_financials = pd.DataFrame(financial_records_for_csv)
            df_financials['report_date'] = pd.to_datetime(df_financials['report_date'])
            df_financials = df_financials.sort_values(by='report_date').reset_index(drop=True)
            
            file_path = os.path.join(FMP_DATA_FOLDER, f"{symbol}_financials.csv")
            ensure_data_folder_exists(FMP_DATA_FOLDER)
            
            df_financials.to_csv(file_path, index=False)
            logger.info(f"[{symbol}] Successfully saved {len(df_financials)} FMP financial statement data points to '{file_path}'.")
        else:
            logger.warning(f"[{symbol}] No financial statement data collected, so not saving to CSV file.")

    except requests.exceptions.RequestException as e:
        logger.error(f"[{symbol}] FMP API request error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[{symbol}] Unexpected error during FMP financial statement data collection: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    logger.info("Running FMP_collector.py script directly (for testing purposes).")

    ensure_data_folder_exists(YFINANCE_DATA_FOLDER)
    ensure_data_folder_exists(FMP_DATA_FOLDER)

    fmp_api_key = config_loader.CONFIG['api_keys'].get('fmp')
    if not fmp_api_key:
        logger.critical("FMP API key not set under 'api_keys' in config.yaml. Skipping FMP test run.")
        exit(1)

    test_stock_symbols = config_loader.CONFIG['data_sources'].get('stocks', [])
    if not test_stock_symbols:
        logger.warning("The 'stocks' list under 'data_sources' in config.yaml is empty. Skipping test run.")
        exit(1)

    today = datetime.now()
    one_year_ago = today - timedelta(days=365)
    
    for symbol in test_stock_symbols[:1]:
        collect_and_save_stock_ohlcv_yfinance(symbol, one_year_ago.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d'))
        time.sleep(1)

    for symbol in test_stock_symbols[:1]: 
        collect_and_save_financials_fmp(symbol, fmp_api_key)
        time.sleep(1) 

    logger.info("FMP_collector.py test run completed.")