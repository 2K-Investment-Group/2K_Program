import requests
import psycopg2
from datetime import datetime, timedelta
import os
import sys
import pandas as pd
import time
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from Data.config import config_loader
from utils.logger_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

BASE_RAW_DATA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
    "raw_data"
)

ALPHA_VANTAGE_DATA_FOLDER = os.path.join(BASE_RAW_DATA_PATH, "alpha_vantage")
CRYPTO_DATA_FOLDER = os.path.join(BASE_RAW_DATA_PATH, "crypto")


def ensure_data_folder_exists(folder_path):
    """Create the specified data folder if it doesn't exist"""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        logger.info(f"'{folder_path}' Created the specified data folder ")

# Database Connection
def get_db_connection():
    """Return the database connection object"""
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
        logger.error(f"Error connecting to the database: {e}")
        raise

# Stock OHLCV Data Collection (AlphaVantage-based)
def collect_and_save_stock_ohlcv_alphavantage(symbol, api_key, outputsize='full'):
    """
    Collect daily OHLCV data for a specified stock using the AlphaVantage API,
    and save it to both the database and a CSV file organized by exchange/industry
    """
    logger.info(f"[{symbol}] Collecting stock OHLCV data from AlphaVantage...")

    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize={outputsize}&apikey={api_key}"

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception:
        logger.error(f"[{symbol}] Failed to connect to OHLCV database; skipping DB save, attempting file save only")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Error Message" in data:
            logger.error(f"[{symbol}] AlphaVantage API error: {data['Error Message']}")
            return
        if "Note" in data:
            logger.warning(f"[{symbol}] AlphaVantage API note: {data['Note']}")
            return
        if "Time Series (Daily)" not in data:
            logger.warning(f"[{symbol}] Unable to find stock data on AlphaVantage. Response: {data}")
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
                logger.error(f"[{symbol}] Data conversion error on {date_str}: {ve}. Data: {values}")
                continue
            except Exception as e:
                logger.error(f"[{symbol}] Error processing stock data on {date_str}: {e}", exc_info=True)
                continue

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
                    logger.info(f"[{symbol}] Successfully saved {db_insert_count} AlphaVantage OHLCV records to the database.")
                except Exception as e:
                    logger.error(f"[{symbol}] Error while saving OHLCV data to the database: {e}", exc_info=True)
                    conn.rollback()
            else:
                logger.warning(f"[{symbol}] No OHLCV data collected; nothing was saved to the database.")
        else:
            logger.warning(f"[{symbol}] OHLCV data was not saved to the database due to a connection issue.")

        if ohlcv_file_records:
            df = pd.DataFrame(ohlcv_file_records)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values(by='timestamp').reset_index(drop=True)

            stock_info = get_stock_info_from_db(symbol)

            exchange_name = stock_info.get('exchange', 'Unknown_Exchange').replace('/', '_').replace('\\', '_') if stock_info else 'Unknown_Exchange'
            industry_name = stock_info.get('industry', 'Unknown_Industry').replace('/', '_').replace('\\', '_') if stock_info else 'Unknown_Industry'

            # Changed file save path: ALPHA_VANTAGE_DATA_FOLDER
            target_folder = os.path.join(ALPHA_VANTAGE_DATA_FOLDER, exchange_name, industry_name, "ohlcv") # OHLCV in separate folder
            ensure_data_folder_exists(target_folder)

            file_path = os.path.join(target_folder, f"{symbol}_ohlcv.csv")
            df.to_csv(file_path, index=False)
            logger.info(f"[{symbol}] Successfully saved {len(df)} AlphaVantage OHLCV records to '{file_path}'.")
        else:
            logger.warning(f"[{symbol}] No OHLCV data collected; nothing was saved to the CSV file.")

    except requests.exceptions.RequestException as e:
        logger.error(f"[{symbol}] AlphaVantage OHLCV API request error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[{symbol}] Unexpected error while collecting AlphaVantage OHLCV data: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# Financials Data Collection
def collect_and_save_financials_alphavantage(symbol, api_key):
    """
    Collects financial statement data for a specified stock using the AlphaVantage API,
    and saves the data to both the database and a CSV file.
    The CSV file is stored in a folder structured by exchange and industry sector.
    """

    logger.info(f"[{symbol}] Starting collection of financial statement data from AlphaVantage...")

    if not api_key:
        logger.warning(f"[{symbol}] AlphaVantage API key not found. Skipping financial statement collection.")
        return

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception:
        logger.error(f"[{symbol}] Failed to connect to the financial statement database; skipping DB save, attempting file save only.")

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
                    logger.error(f"[{symbol}] AlphaVantage {stmt_type} API error: {data['Error Message']}")
                    financial_statements_raw[stmt_type] = []
                    continue
                if "Note" in data:
                    logger.warning(f"[{symbol}] AlphaVantage {stmt_type} API note: {data['Note']}")
                    financial_statements_raw[stmt_type] = []
                    continue

                quarterly_reports = data.get('quarterlyReports', [])
                annual_reports = data.get('annualReports', [])

                if quarterly_reports:
                    financial_statements_raw[stmt_type] = [{'report': r, 'period_type': 'quarterly'} for r in quarterly_reports]
                elif annual_reports:
                    financial_statements_raw[stmt_type] = [{'report': r, 'period_type': 'annual'} for r in annual_reports]
                else:
                    logger.warning(f"[{symbol}] No quarterly/annual reports found for AlphaVantage {stmt_type}. Response: {data}")
                    financial_statements_raw[stmt_type] = []

            except requests.exceptions.RequestException as e:
                logger.error(f"[{symbol}] AlphaVantage {stmt_type} API request error: {e}", exc_info=True)
                financial_statements_raw[stmt_type] = []
            except Exception as e:
                logger.error(f"[{symbol}] Unexpected error while parsing AlphaVantage {stmt_type} data: {e}", exc_info=True)
                financial_statements_raw[stmt_type] = []
            finally:
                time.sleep(15)

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
            logger.warning(f"[{symbol}] No combined financial statement data found from AlphaVantage.")
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
                logger.error(f"[{symbol}] Data conversion error for financial statement {date_str}: {ve}. Data: {statement}")
                continue
            except Exception as e:
                logger.error(f"[{symbol}] Error processing financial statement data for {date_str}: {e}", exc_info=True)
                continue

        # Save to database
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
                    logger.info(f"[{symbol}] Successfully saved {db_insert_count} AlphaVantage financial statement records to the database.")
                except Exception as e:
                    logger.error(f"[{symbol}] Error while saving financial statement data to the database: {e}", exc_info=True)
                    conn.rollback()
            else:
                logger.warning(f"[{symbol}] No financial statement data collected; nothing was saved to the database.")
        else:
            logger.warning(f"[{symbol}] Financial statement data was not saved to the database due to a connection issue.")

        # Save to CSV file
        if financial_file_records:
            df = pd.DataFrame(financial_file_records)
            df['report_date'] = pd.to_datetime(df['report_date'])
            df = df.sort_values(by='report_date').reset_index(drop=True)

            # Retrieve stock information (exchange, industry sector)
            stock_info = get_stock_info_from_db(symbol)

            exchange_name = stock_info.get('exchange', 'Unknown_Exchange').replace('/', '_').replace('\\', '_') if stock_info else 'Unknown_Exchange'
            industry_name = stock_info.get('industry', 'Unknown_Industry').replace('/', '_').replace('\\', '_') if stock_info else 'Unknown_Industry'

            # Changed file save path
            target_folder = os.path.join(ALPHA_VANTAGE_DATA_FOLDER, exchange_name, industry_name, "financials") # Financials in separate folder
            ensure_data_folder_exists(target_folder)

            file_path = os.path.join(target_folder, f"{symbol}_financials.csv")
            df.to_csv(file_path, index=False)
            logger.info(f"[{symbol}] Successfully saved {len(df)} AlphaVantage financial statement records to '{file_path}'.")
        else:
            logger.warning(f"[{symbol}] No financial statement data collected; nothing was saved to the CSV file.")

    except requests.exceptions.RequestException as e:
        logger.error(f"[{symbol}] AlphaVantage Financials API request error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[{symbol}] Unexpected error while collecting financial statement data: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# Stock Company Overview Data Collection
def collect_stock_info_alphavantage(symbol, api_key):
    logger.info(f"[{symbol}] Starting collection of company overview data from AlphaVantage...")

    if not api_key:
        logger.warning(f"[{symbol}] AlphaVantage API key not found. Skipping company overview collection.")
        return {}

    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={api_key}"

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception:
        logger.error(f"[{symbol}] Failed to connect to company overview database; skipping DB save, returning API response only.")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Error Message" in data:
            logger.error(f"[{symbol}] AlphaVantage Company Overview API error: {data['Error Message']}")
            return {}
        if not data or len(data) == 0:
            logger.warning(f"[{symbol}] No company overview data found from AlphaVantage. Response: {data}")
            return {}

        # Save to database
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
                logger.info(f"[{symbol}] Successfully saved/updated company overview data to the database.")
            except Exception as e:
                logger.error(f"[{symbol}] Error while saving company overview data to the database: {e}", exc_info=True)
                conn.rollback()
        else:
            logger.warning(f"[{symbol}] Company overview data was not saved to the DB due to a connection issue.")

        return {
            'symbol': symbol,
            'name': data.get('Name'),
            'exchange': data.get('Exchange'),
            'sector': data.get('Sector'),
            'industry': data.get('Industry')
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"[{symbol}] AlphaVantage Company Overview API request error: {e}", exc_info=True)
        return {}
    except Exception as e:
        logger.error(f"[{symbol}] Unexpected error while collecting company overview data: {e}", exc_info=True)
        return {}
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_stock_info_from_db(symbol):
    """
    Retrieves exchange and industry sector information for a specific stock from the database.
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
        return {'exchange': 'Unknown_Exchange', 'industry': 'Unknown_Industry'}
    except Exception as e:
        logger.error(f"[{symbol}] Error fetching stock information from the database: {e}", exc_info=True)
        return {'exchange': 'Error_Exchange', 'industry': 'Error_Industry'}
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# Crypto OHLCV Data Collection (ccxt based - existing code maintained)
import ccxt

def collect_and_save_crypto_ohlcv(symbol, since_days=365, exchange_id='binance'):
    """
    Collects cryptocurrency OHLCV data using the ccxt library and
    saves it to both the database and a CSV file.
    """
    logger.info(f"[{symbol}] Starting cryptocurrency OHLCV data collection from {exchange_id}...")
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class()

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception:
        logger.error(f"[{symbol}] Failed to connect to the cryptocurrency database; skipping DB save, attempting file save only.")

    try:
        since_timestamp_ms = exchange.parse8601((datetime.now() - timedelta(days=since_days)).isoformat())

        ohlcv = exchange.fetch_ohlcv(symbol, '1d', since=since_timestamp_ms)
        if not ohlcv:
            logger.warning(f"[{symbol}] No cryptocurrency data found from {exchange_id}.")
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

        # Save to database
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
                    logger.info(f"[{symbol}] Successfully saved {db_insert_count} cryptocurrency OHLCV records from {exchange_id} to the database.")
                except Exception as e:
                    logger.error(f"[{symbol}] Error while saving cryptocurrency OHLCV data to the database: {e}", exc_info=True)
                    conn.rollback()
            else:
                logger.warning(f"[{symbol}] No cryptocurrency OHLCV data collected; nothing was saved to the database.")
        else:
            logger.warning(f"[{symbol}] Cryptocurrency OHLCV data was not saved to the DB due to a connection issue.")

        # Save to CSV file
        if crypto_file_records:
            df = pd.DataFrame(crypto_file_records)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values(by='timestamp').reset_index(drop=True)

            # Changed file save path
            file_path = os.path.join(CRYPTO_DATA_FOLDER, f"{symbol.replace('/', '_')}_ohlcv.csv") # For crypto, OHLCV folder structure might be unnecessary, direct filename
            ensure_data_folder_exists(os.path.dirname(file_path))

            df.to_csv(file_path, index=False)
            logger.info(f"[{symbol}] Successfully saved {len(df)} cryptocurrency OHLCV records from {exchange_id} to '{file_path}'.")
        else:
            logger.warning(f"[{symbol}] No cryptocurrency OHLCV data collected; nothing was saved to the CSV file.")

    except ccxt.NetworkError as e:
        logger.error(f"[{symbol}] Network error while fetching cryptocurrency data from {exchange_id}: {e}")
    except ccxt.ExchangeError as e:
        logger.error(f"[{symbol}] Exchange error while fetching cryptocurrency data from {exchange_id}: {e}")
    except Exception as e:
        logger.error(f"[{symbol}] Unexpected error while collecting/saving cryptocurrency OHLCV data: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    logger.info("Starting AlphaVantage & Crypto data collection script (DB and file simultaneous save mode).")

    ensure_data_folder_exists(ALPHA_VANTAGE_DATA_FOLDER)
    ensure_data_folder_exists(CRYPTO_DATA_FOLDER)

    alphavantage_api_key = config_loader.CONFIG['api_keys'].get('alphavantage')
    if not alphavantage_api_key:
        logger.critical("AlphaVantage API key not configured under 'api_keys' in config.yaml. Exiting script.")
        exit(1)

    stock_symbols = config_loader.CONFIG['data_sources'].get('stocks', [])
    if not stock_symbols:
        logger.warning("No 'stocks' list found under 'data_sources' in config.yaml. Skipping stock data collection.")

    # Collect Stock Company Overview (Exchange, Industry Sector) Data
    logger.info("Starting stock company overview data collection.")
    for symbol in stock_symbols:
        collect_stock_info_alphavantage(symbol, alphavantage_api_key)
        logger.info(f"Waiting 15 seconds after processing {symbol} company overview for API call limit.")
        time.sleep(15)
    logger.info("All stock company overview data collection completed.")

    # Collect Stock OHLCV Data
    logger.info("Starting stock OHLCV data collection.")
    for symbol in stock_symbols:
        collect_and_save_stock_ohlcv_alphavantage(symbol, alphavantage_api_key)
        logger.info(f"Waiting 15 seconds after processing {symbol} OHLCV for API call limit.")
        time.sleep(15)
    logger.info("All stock OHLCV data collection completed.")

    # Collect Stock Financial Statement Data
    logger.info("Starting stock financial statement data collection.")
    for symbol in stock_symbols:
        collect_and_save_financials_alphavantage(symbol, alphavantage_api_key)
        logger.info(f"Waiting 15 seconds after processing {symbol} financials for API call limit.")
        time.sleep(15)
    logger.info("All stock financial statement data collection completed.")

    crypto_symbols = config_loader.CONFIG['data_sources'].get('cryptos', [])
    if not crypto_symbols:
        logger.warning("No 'cryptos' list found under 'data_sources' in config.yaml. Skipping cryptocurrency data collection.")

    logger.info("Starting cryptocurrency OHLCV data collection.")
    for crypto_symbol in crypto_symbols:
        collect_and_save_crypto_ohlcv(crypto_symbol, since_days=365)

    logger.info("All cryptocurrency data collection completed.")

    logger.info("Data collection script finished (DB and file simultaneous save mode).")