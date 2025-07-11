# data_collector.py (제공해주신 코드 그대로 사용)

import yfinance as yf
import ccxt
import requests
import psycopg2
from datetime import datetime, timedelta
from config_loader import CONFIG # <--- 여기가 핵심! config_loader.py에서 CONFIG를 가져옵니다.
import logging
from logger_config import setup_logging
import pandas as pd
import numpy as np # Make sure numpy is imported if you're using it elsewhere

setup_logging()
logger = logging.getLogger(__name__)

# --- Database Connection ---
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=CONFIG['database']['host'],
            database=CONFIG['database']['dbname'],
            user=CONFIG['database']['user'],
            password=CONFIG['database']['password'], # config_loader.py에서 이미 환경변수 값으로 대체됨
            port=CONFIG['database']['port']
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        raise

# --- Stock OHLCV Data Collection ---
def collect_and_save_stock_ohlcv(symbol, start_date, end_date):
    logger.info(f"Collecting stock OHLCV for {symbol} from {start_date} to {end_date}...")
    try:
        data = yf.download(symbol, start=start_date, end=end_date)
        if data.empty:
            logger.warning(f"No stock data found for {symbol} in the specified range.")
            return

        conn = None
        cur = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            for index, row in data.iterrows():
                trade_date = index.to_pydatetime()
                open_price = float(row['Open']) if not pd.isna(row['Open']) else None
                high_price = float(row['High']) if not pd.isna(row['High']) else None
                low_price = float(row['Low']) if not pd.isna(row['Low']) else None
                close_price = float(row['Close']) if not pd.isna(row['Close']) else None
                volume = int(row['Volume']) if not pd.isna(row['Volume']) else None

                cur.execute("""
                    INSERT INTO stock_ohlcv (
                        symbol, trade_date, open, high, low, close, volume
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, trade_date) DO UPDATE
                    SET open = EXCLUDED.open, high = EXCLUDED.high,
                        low = EXCLUDED.low, close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                """, (
                    symbol,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume
                ))
            conn.commit()
            logger.info(f"Successfully saved stock OHLCV for {symbol}.")

        except Exception as e:
            logger.error(f"Error inserting/updating stock data for {symbol} at {trade_date}: {e}", exc_info=True)
            if conn:
                conn.rollback()
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    except Exception as e:
        logger.error(f"Error collecting stock OHLCV for {symbol}: {e}")


# --- Crypto OHLCV Data Collection ---
def collect_and_save_crypto_ohlcv(symbol, since_days=365, exchange_id='binance'):
    logger.info(f"Collecting crypto OHLCV for {symbol} from {exchange_id}...")
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class()

    try:
        since_timestamp_ms = exchange.parse8601((datetime.now() - timedelta(days=since_days)).isoformat())

        ohlcv = exchange.fetch_ohlcv(symbol, '1d', since=since_timestamp_ms)
        if not ohlcv:
            logger.warning(f"No crypto data found for {symbol} on {exchange_id}.")
            return

        conn = None
        cur = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            for candle in ohlcv:
                trade_date = datetime.fromtimestamp(candle[0] / 1000).date()
                open_price = float(candle[1]) if candle[1] is not None else None
                high_price = float(candle[2]) if candle[2] is not None else None
                low_price = float(candle[3]) if candle[3] is not None else None
                close_price = float(candle[4]) if candle[4] is not None else None
                volume = float(candle[5]) if candle[5] is not None else None

                cur.execute("""
                    INSERT INTO crypto_ohlcv (
                        symbol, trade_date, open, high, low, close, volume, exchange
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, trade_date, exchange) DO UPDATE
                    SET open = EXCLUDED.open, high = EXCLUDED.high,
                        low = EXCLUDED.low, close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                """, (
                    symbol,
                    trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    exchange_id
                ))
            conn.commit()
            logger.info(f"Successfully saved crypto OHLCV for {symbol}.")

        except Exception as e:
            logger.error(f"Error inserting/updating crypto data for {symbol} on {exchange_id}: {e}", exc_info=True)
            if conn:
                conn.rollback()
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    except ccxt.NetworkError as e:
        logger.error(f"Network error while fetching crypto data for {symbol} from {exchange_id}: {e}")
    except ccxt.ExchangeError as e:
        logger.error(f"Exchange error while fetching crypto data for {symbol} from {exchange_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error collecting crypto OHLCV for {symbol}: {e}")

# --- Financials Data Collection (Financial Modeling Prep Example) ---
def collect_and_save_financials(symbol, api_key_name_in_config): # 함수 인자 이름을 변경하여 혼란 방지
    logger.info(f"Collecting financials for {symbol} using API key from '{api_key_name_in_config}'...")
    # config_loader.py에서 처리했기 때문에 CONFIG['api_keys']에는 이미 실제 키 값이 들어있습니다.
    # config.yaml의 financial_modeling_prep_env -> config_loader에서 financial_modeling_prep으로 변경됨
    api_key = CONFIG['api_keys'].get(api_key_name_in_config.replace('_env', '')) # '_env' 제거된 키로 접근

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
            logger.warning(f"No financials found for {symbol} after combining. This might indicate API issues or no data.")
            return

        for date_str, statement in combined_financials.items():
            report_date = datetime.strptime(date_str.split('T')[0], '%Y-%m-%d').date()
            period_type = 'quarterly'

            try:
                revenue = float(statement.get('revenue')) if statement.get('revenue') is not None else None
                gross_profit = float(statement.get('grossProfit')) if statement.get('grossProfit') is not None else None
                operating_income = float(statement.get('operatingIncome')) if statement.get('operatingIncome') is not None else None
                net_income = float(statement.get('netIncome')) if statement.get('netIncome') is not None else None
                total_assets = float(statement.get('totalAssets')) if statement.get('totalAssets') is not None else None
                total_liabilities = float(statement.get('totalLiabilities')) if statement.get('totalLiabilities') is not None else None
                total_equity = float(statement.get('totalEquity')) if statement.get('totalEquity') is not None else None
                cash_from_operations = float(statement.get('cashFlowFromOperatingActivities')) if statement.get('cashFlowFromOperatingActivities') is not None else None

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
                    symbol,
                    report_date,
                    period_type,
                    revenue,
                    gross_profit,
                    operating_income,
                    net_income,
                    total_assets,
                    total_liabilities,
                    total_equity,
                    cash_from_operations
                ))
            except Exception as e:
                logger.error(f"Error inserting/updating financial data for {symbol} on {report_date} ({period_type}): {e}")
                if conn:
                    conn.rollback()
                continue

        conn.commit()
        logger.info(f"Successfully saved financials for {symbol}.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Network or API connectivity error for {symbol}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error collecting financials for {symbol}: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Main Execution ---
if __name__ == "__main__":
    today = datetime.now()
    one_year_ago = today - timedelta(days=365)

    for stock_symbol in CONFIG['data_sources']['stocks']:
        collect_and_save_stock_ohlcv(stock_symbol, one_year_ago.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d'))

    for crypto_symbol in CONFIG['data_sources']['cryptos']:
        collect_and_save_crypto_ohlcv(crypto_symbol, since_days=365)

    for stock_symbol in CONFIG['data_sources']['stocks']:
        # 'financial_modeling_prep_env'는 config.yaml에 있는 키 이름입니다.
        # config_loader.py가 이를 처리하여 CONFIG['api_keys']['financial_modeling_prep']에 실제 키를 넣어줍니다.
        # 따라서 collect_and_save_financials 함수 내부에서 '_env'를 제거하여 접근해야 합니다.
        collect_and_save_financials(stock_symbol, 'financial_modeling_prep_env')