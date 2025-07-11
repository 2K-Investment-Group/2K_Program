# db_setup.py
import psycopg2
from Data.config.config_loader import CONFIG

def create_tables():
    conn = None
    try:
        conn = psycopg2.connect(
            host=CONFIG['database']['host'],
            port=CONFIG['database']['port'],
            user=CONFIG['database']['user'],
            password=CONFIG['database']['password_env'], # .env에서 로드된 값 사용
            dbname=CONFIG['database']['dbname'] # .env에서 로드된 값 사용
        )
        cur = conn.cursor()

        # 주식 OHLCV 테이블 (TimescaleDB Hypertable)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_ohlcv (
                symbol VARCHAR(20) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume BIGINT,
                PRIMARY KEY (symbol, timestamp)
            );
            SELECT create_hypertable('stock_ohlcv', 'timestamp', if_not_exists => TRUE);
        """)

        # 암호화폐 OHLCV 테이블 (TimescaleDB Hypertable)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_ohlcv (
                symbol VARCHAR(20) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume NUMERIC,
                PRIMARY KEY (symbol, timestamp)
            );
            SELECT create_hypertable('crypto_ohlcv', 'timestamp', if_not_exists => TRUE);
        """)

        # 재무 데이터 테이블
        cur.execute("""
            CREATE TABLE IF NOT EXISTS financials (
                symbol VARCHAR(20) NOT NULL,
                report_date DATE NOT NULL,
                period VARCHAR(10) NOT NULL, -- 'annual', 'quarterly'
                revenue NUMERIC,
                gross_profit NUMERIC,
                operating_income NUMERIC,
                net_income NUMERIC,
                total_assets NUMERIC,
                total_liabilities NUMERIC,
                total_equity NUMERIC,
                cash_from_operations NUMERIC,
                PRIMARY KEY (symbol, report_date, period)
            );
        """)

        conn.commit()
        print("Tables created or already exist.")

    except Exception as e:
        print(f"Error creating tables: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    create_tables()