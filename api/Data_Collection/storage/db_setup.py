import os
import sys
import logging
from sqlalchemy import create_engine, text

# 프로젝트 루트 디렉토리를 PYTHONPATH에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from Data.config import config_loader
from utils.logger_config import setup_logging
from storage.db_utils import get_db_engine # db_utils에서 engine 가져오기

logger = logging.getLogger(__name__)

def setup_database():
    logger.info("데이터베이스 설정 시작...")
    
    engine = get_db_engine()
    if not engine:
        logger.critical("DB 엔진을 가져올 수 없어 데이터베이스 설정을 계속할 수 없습니다.")
        return

    try:
        with engine.connect() as connection:
            # TimescaleDB 확장 기능 활성화 (PostgreSQL superuser 권한 필요)
            # 만약 권한 문제가 있다면, DB 관리자에게 직접 이 명령을 실행해달라고 요청해야 합니다.
            connection.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
            logger.info("TimescaleDB 확장 기능 활성화 완료.")
            connection.commit() # DDL 문은 commit 필요

            # --- Alpha Vantage 재무제표 테이블 생성 (이전과 동일) ---
            common_fs_cols = """
                symbol VARCHAR(10) NOT NULL,
                fiscal_date_ending DATE NOT NULL,
                reported_currency VARCHAR(10),
                reported_date DATE,
                period_type VARCHAR(10), -- 'annual', 'quarterly'
            """
            
            # INCOME_STATEMENT
            income_statement_cols = common_fs_cols + """
                gross_profit NUMERIC,
                total_revenue NUMERIC,
                cost_of_revenue NUMERIC,
                cost_of_goods_and_services_sold NUMERIC,
                operating_income NUMERIC,
                selling_general_and_administrative NUMERIC,
                research_and_development NUMERIC,
                operating_expenses NUMERIC,
                investment_income NUMERIC,
                net_interest_income NUMERIC,
                other_non_operating_income NUMERIC,
                depreciation NUMERIC,
                amortization NUMERIC,
                depreciation_and_amortization NUMERIC,
                income_before_tax NUMERIC,
                income_tax_expense NUMERIC,
                interest_and_debt_expense NUMERIC,
                net_income NUMERIC,
                comprehensive_income_fcf NUMERIC,
                ebit NUMERIC,
                ebitda NUMERIC,
                net_income_from_continuing_operations NUMERIC,
                accepted_date DATE,
                non_operating_income NUMERIC,
                total_other_income_expense NUMERIC,
                discontinued_operations NUMERIC,
                extraordinary_items NUMERIC,
                effect_of_accounting_changes NUMERIC,
                other_income_expense NUMERIC,
                non_recurring_income NUMERIC,
                gain_on_sale_of_assets NUMERIC,
                loss_on_sale_of_assets NUMERIC,
                other_gains_losses NUMERIC,
                unusual_items NUMERIC,
                preferred_dividends NUMERIC,
                basic_eps NUMERIC,
                diluted_eps NUMERIC,
                weighted_average_shares NUMERIC,
                weighted_average_shares_diluted NUMERIC
            """
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS alpha_vantage_income_statements_raw (
                id SERIAL PRIMARY KEY,
                {income_statement_cols}
                CONSTRAINT unique_income_statement UNIQUE (symbol, fiscal_date_ending, period_type)
            );
            """
            connection.execute(text(create_table_sql))
            connection.execute(text("SELECT create_hypertable('alpha_vantage_income_statements_raw', 'fiscal_date_ending', if_not_exists => TRUE);"))
            logger.info("테이블 'alpha_vantage_income_statements_raw' 생성 및 하이퍼테이블 설정 완료.")
            connection.commit()

            # BALANCE_SHEET
            balance_sheet_cols = common_fs_cols + """
                total_assets NUMERIC,
                current_assets NUMERIC,
                cash_and_cash_equivalents NUMERIC,
                short_term_investments NUMERIC,
                receivables NUMERIC,
                inventory NUMERIC,
                other_current_assets NUMERIC,
                non_current_assets NUMERIC,
                property_plant_and_equipment NUMERIC,
                accumulated_depreciation_amortization NUMERIC,
                intangible_assets NUMERIC,
                goodwill NUMERIC,
                long_term_investments NUMERIC,
                other_non_current_assets NUMERIC,
                total_liabilities NUMERIC,
                current_liabilities NUMERIC,
                accounts_payable NUMERIC,
                short_term_debt NUMERIC,
                other_current_liabilities NUMERIC,
                long_term_debt NUMERIC,
                other_non_current_liabilities NUMERIC,
                total_shareholder_equity NUMERIC,
                common_stock NUMERIC,
                retained_earnings NUMERIC,
                treasury_stock NUMERIC,
                capital_surplus NUMERIC,
                other_shareholder_equity NUMERIC,
                accepted_date DATE
            """
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS alpha_vantage_balance_sheets_raw (
                id SERIAL PRIMARY KEY,
                {balance_sheet_cols}
                CONSTRAINT unique_balance_sheet UNIQUE (symbol, fiscal_date_ending, period_type)
            );
            """
            connection.execute(text(create_table_sql))
            connection.execute(text("SELECT create_hypertable('alpha_vantage_balance_sheets_raw', 'fiscal_date_ending', if_not_exists => TRUE);"))
            logger.info("테이블 'alpha_vantage_balance_sheets_raw' 생성 및 하이퍼테이블 설정 완료.")
            connection.commit()

            # CASH_FLOW
            cash_flow_cols = common_fs_cols + """
                operating_cashflow NUMERIC,
                payments_for_operating_activities NUMERIC,
                proceeds_from_operating_activities NUMERIC,
                change_in_operating_liabilities NUMERIC,
                change_in_operating_assets NUMERIC,
                depreciation_depletion_and_amortization NUMERIC,
                capital_expenditures NUMERIC,
                investments_cashflow NUMERIC,
                other_investing_cashflow NUMERIC,
                financing_cashflow NUMERIC,
                proceeds_from_repayments_of_short_term_debt NUMERIC,
                payments_for_repurchase_of_common_stock NUMERIC,
                payments_for_repurchase_of_equity NUMERIC,
                payments_for_repurchase_of_preferred_stock NUMERIC,
                dividend_payout NUMERIC,
                dividend_payout_common_stock NUMERIC,
                dividend_payout_preferred_stock NUMERIC,
                proceeds_from_issuance_of_common_stock NUMERIC,
                proceeds_from_issuance_of_long_term_debt_and_capital_lease_obligations NUMERIC,
                proceeds_from_issuance_of_preferred_stock NUMERIC,
                proceeds_from_issuance_of_debt NUMERIC,
                proceeds_from_issuance_of_equity NUMERIC,
                other_financing_cashflow NUMERIC,
                effect_of_forex_on_cash NUMERIC,
                net_income NUMERIC,
                change_in_cash_and_cash_equivalents NUMERIC,
                accepted_date DATE
            """
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS alpha_vantage_cash_flows_raw (
                id SERIAL PRIMARY KEY,
                {cash_flow_cols}
                CONSTRAINT unique_cash_flow UNIQUE (symbol, fiscal_date_ending, period_type)
            );
            """
            connection.execute(text(create_table_sql))
            connection.execute(text("SELECT create_hypertable('alpha_vantage_cash_flows_raw', 'fiscal_date_ending', if_not_exists => TRUE);"))
            logger.info("테이블 'alpha_vantage_cash_flows_raw' 생성 및 하이퍼테이블 설정 완료.")
            connection.commit()

            # --- Alpha Vantage Daily Prices (OHLCV) 테이블 생성 ---
            # 새로운 테이블
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS alpha_vantage_daily_prices_raw (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                trade_date DATE NOT NULL,
                open_price NUMERIC,
                high_price NUMERIC,
                low_price NUMERIC,
                close_price NUMERIC,
                volume BIGINT,
                CONSTRAINT unique_daily_price UNIQUE (symbol, trade_date)
            );
            """
            connection.execute(text(create_table_sql))
            connection.execute(text("SELECT create_hypertable('alpha_vantage_daily_prices_raw', 'trade_date', if_not_exists => TRUE);"))
            logger.info("테이블 'alpha_vantage_daily_prices_raw' 생성 및 하이퍼테이블 설정 완료.")
            connection.commit()

            # --- FRED Series 테이블 생성 ---
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS fred_series_raw (
                id SERIAL PRIMARY KEY,
                series_id VARCHAR(50) NOT NULL,
                date DATE NOT NULL,
                value NUMERIC,
                # FRED API에서 추가로 제공할 수 있는 메타데이터 필드 (필요시 추가)
                # observation_start_date DATE,
                # observation_end_date DATE,
                # frequency VARCHAR(20),
                # units VARCHAR(50),
                # title VARCHAR(255),
                CONSTRAINT unique_fred_series UNIQUE (series_id, date)
            );
            """
            connection.execute(text(create_table_sql))
            connection.execute(text("SELECT create_hypertable('fred_series_raw', 'date', if_not_exists => TRUE);"))
            logger.info("테이블 'fred_series_raw' 생성 및 하이퍼테이블 설정 완료.")
            connection.commit()

            # --- World Bank Indicators 테이블 생성 ---
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS world_bank_indicators_raw (
                id SERIAL PRIMARY KEY,
                country_name VARCHAR(255) NOT NULL,
                country_code VARCHAR(10) NOT NULL,
                indicator_name TEXT NOT NULL,
                indicator_code VARCHAR(50) NOT NULL,
                year INTEGER NOT NULL,
                value NUMERIC,
                CONSTRAINT unique_wb_indicator UNIQUE (country_code, indicator_code, year)
            );
            """
            connection.execute(text(create_table_sql))
            # World Bank 데이터는 'year' 컬럼이므로, 이것을 TimescaleDB 시간축으로 사용.
            # 하지만 TimescaleDB는 DATE/TIMESTAMP 타입을 권장. INTEGER year도 가능하지만 주의.
            # 데이터 수집 시 'year'를 'date' 타입으로 변환하여 저장하는 것이 더 좋습니다.
            # (예: 'YYYY-01-01' 같은 형식으로)
            # 여기서는 일단 INTEGER year로 하이퍼테이블을 만들고,
            # World Bank 데이터를 DB에 저장할 때 'year'를 'YYYY-01-01' 같은 'date'로 변환하도록 하겠습니다.
            # 테이블 컬럼도 year 대신 date DATE NOT NULL로 바꾸는 게 더 좋습니다.

            # World Bank 테이블 스키마 재수정 (year -> date DATE)
            create_table_sql = """
            DROP TABLE IF EXISTS world_bank_indicators_raw; -- 기존 테이블이 있다면 삭제 후 재생성 (스키마 변경을 위해)
            CREATE TABLE IF NOT EXISTS world_bank_indicators_raw (
                id SERIAL PRIMARY KEY,
                country_name VARCHAR(255) NOT NULL,
                country_code VARCHAR(10) NOT NULL,
                indicator_name TEXT NOT NULL,
                indicator_code VARCHAR(50) NOT NULL,
                date DATE NOT NULL, -- year 대신 date 컬럼으로 변경
                value NUMERIC,
                CONSTRAINT unique_wb_indicator UNIQUE (country_code, indicator_code, date)
            );
            """
            connection.execute(text(create_table_sql))
            connection.execute(text("SELECT create_hypertable('world_bank_indicators_raw', 'date', if_not_exists => TRUE);"))
            logger.info("테이블 'world_bank_indicators_raw' 생성 및 하이퍼테이블 설정 완료.")
            connection.commit()


            logger.info("모든 데이터베이스 설정 완료.")

    except Exception as e:
        logger.error(f"데이터베이스 설정 중 오류 발생: {e}", exc_info=True)
    finally:
        if 'engine' in locals() and engine:
            engine.dispose()

if __name__ == "__main__":
    setup_logging()
    setup_database()