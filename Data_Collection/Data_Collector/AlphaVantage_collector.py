import requests
import os
import sys
import pandas as pd
import time
import logging
from datetime import datetime, timedelta

# SQLAlchemy imports
from sqlalchemy import create_engine, Column, Integer, String, Numeric, Date, Text, BigInteger, DateTime, UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert as pg_insert # PostgreSQL ON CONFLICT를 위해 필요

# 현재 파일의 디렉토리와 프로젝트 루트 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

# sys.path에 프로젝트 루트 추가
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 사용자 정의 모듈 임포트
from Data_Collection.config import config_loader
from utils.logger_config import setup_logging

# 로깅 설정
setup_logging()
logger = logging.getLogger(__name__)

# 데이터 저장 경로 설정
BASE_RAW_DATA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
    "raw_data"
)
ALPHA_VANTAGE_DATA_FOLDER = os.path.join(BASE_RAW_DATA_PATH, "alpha_vantage")
# CRYPTO_DATA_FOLDER = os.path.join(BASE_RAW_DATA_PATH, "crypto") # 현재 요청 스키마에 없으므로 주석 처리

def ensure_data_folder_exists(folder_path):
    """지정된 데이터 폴더가 없으면 생성합니다."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        logger.info(f"'{folder_path}' 데이터 폴더를 생성했습니다.")

# --- SQLAlchemy 설정 및 모델 정의 (새로운 스키마에 맞춤) ---
Base = declarative_base()

# DB 연결 URL 생성
DB_CONFIG = config_loader.CONFIG['database']
DATABASE_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def get_db_session():
    """데이터베이스 세션 객체를 반환합니다."""
    try:
        session = Session()
        return session
    except Exception as e:
        logger.error(f"데이터베이스 세션 생성 오류: {e}")
        raise

def create_tables():
    """정의된 모든 SQLAlchemy 모델에 따라 데이터베이스 테이블을 생성합니다."""
    try:
        Base.metadata.create_all(engine)
        logger.info("모든 데이터베이스 테이블이 성공적으로 생성되거나 이미 존재합니다.")
    except Exception as e:
        logger.error(f"데이터베이스 테이블 생성 오류: {e}")
        raise

# 1. alpha_vantage_daily_ohlcv_raw (주식 일별 OHLCV)
class AlphaVantageDailyOHLCVRaw(Base):
    __tablename__ = 'alpha_vantage_daily_ohlcv_raw'
    id = Column(Integer, primary_key=True, autoincrement=True) # PK
    symbol = Column(String(20), nullable=False)
    date = Column(Date, nullable=False) # TimescaleDB 시간축
    open = Column(Numeric)
    high = Column(Numeric)
    low = Column(Numeric)
    close = Column(Numeric)
    volume = Column(BigInteger)
    # 다른 모든 필드는 여기에 추가 가능
    __table_args__ = (UniqueConstraint('symbol', 'date', name='_alpha_ohlcv_symbol_date_uc'),) # 중복 방지

    def __repr__(self):
        return f"<AlphaVantageDailyOHLCVRaw(symbol='{self.symbol}', date='{self.date}')>"

# 2. alpha_vantage_income_statements_raw (손익계산서)
class AlphaVantageIncomeStatementsRaw(Base):
    __tablename__ = 'alpha_vantage_income_statements_raw'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    fiscal_date_ending = Column(Date, nullable=False) # TimescaleDB 시간축
    reported_currency = Column(String(10))
    reported_date = Column(Date)
    period_type = Column(String(20)) # annual, quarterly

    # API 필드 (예시, 실제 API 문서 참고하여 모든 필드 추가)
    gross_profit = Column(Numeric)
    total_revenue = Column(Numeric)
    cost_of_revenue = Column(Numeric)
    operating_income = Column(Numeric)
    operating_expenses = Column(Numeric)
    selling_general_and_administrative = Column(Numeric)
    research_and_development = Column(Numeric)
    depreciation_and_amortization = Column(Numeric)
    income_before_tax = Column(Numeric)
    net_income = Column(Numeric)
    # ... Alpha Vantage Income Statement의 모든 필드
    # 예시:
    ebitda = Column(Numeric)
    eps = Column(Numeric)
    # ...

    __table_args__ = (UniqueConstraint('symbol', 'fiscal_date_ending', 'period_type', name='_alpha_income_uc'),)

    def __repr__(self):
        return f"<AlphaVantageIncomeStatementsRaw(symbol='{self.symbol}', fiscal_date_ending='{self.fiscal_date_ending}', period_type='{self.period_type}')>"

# 3. alpha_vantage_balance_sheets_raw (재무상태표)
class AlphaVantageBalanceSheetsRaw(Base):
    __tablename__ = 'alpha_vantage_balance_sheets_raw'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    fiscal_date_ending = Column(Date, nullable=False) # TimescaleDB 시간축
    reported_currency = Column(String(10))
    reported_date = Column(Date)
    period_type = Column(String(20))

    # API 필드 (예시, 실제 API 문서 참고하여 모든 필드 추가)
    total_assets = Column(Numeric)
    current_assets = Column(Numeric)
    cash_and_cash_equivalents = Column(Numeric)
    net_receivables = Column(Numeric)
    inventory = Column(Numeric)
    total_non_current_assets = Column(Numeric)
    property_plant_and_equipment = Column(Numeric)
    intangible_assets = Column(Numeric)
    total_liabilities = Column(Numeric)
    current_liabilities = Column(Numeric)
    current_accounts_payable = Column(Numeric)
    short_term_debt = Column(Numeric)
    total_non_current_liabilities = Column(Numeric)
    long_term_debt = Column(Numeric)
    total_shareholder_equity = Column(Numeric)
    # ... Alpha Vantage Balance Sheet의 모든 필드
    # 예시:
    retained_earnings = Column(Numeric)
    common_stock = Column(Numeric)
    # ...

    __table_args__ = (UniqueConstraint('symbol', 'fiscal_date_ending', 'period_type', name='_alpha_balance_uc'),)

    def __repr__(self):
        return f"<AlphaVantageBalanceSheetsRaw(symbol='{self.symbol}', fiscal_date_ending='{self.fiscal_date_ending}', period_type='{self.period_type}')>"

# 4. alpha_vantage_cash_flows_raw (현금흐름표)
class AlphaVantageCashFlowsRaw(Base):
    __tablename__ = 'alpha_vantage_cash_flows_raw'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    fiscal_date_ending = Column(Date, nullable=False) # TimescaleDB 시간축
    reported_currency = Column(String(10))
    reported_date = Column(Date)
    period_type = Column(String(20))

    # API 필드 (예시, 실제 API 문서 참고하여 모든 필드 추가)
    operating_cashflow = Column(Numeric)
    payments_for_operating_activities = Column(Numeric)
    proceeds_from_operating_activities = Column(Numeric)
    change_in_operating_liabilities = Column(Numeric)
    change_in_operating_assets = Column(Numeric)
    depreciation_depletion_and_amortization = Column(Numeric)
    capital_expenditures = Column(Numeric)
    investments_cashflow = Column(Numeric)
    dividends_paid = Column(Numeric)
    net_borrowings = Column(Numeric)
    other_cash_flow_from_financing_activities = Column(Numeric)
    # ... Alpha Vantage Cash Flow의 모든 필드
    # 예시:
    free_cash_flow = Column(Numeric)
    # ...

    __table_args__ = (UniqueConstraint('symbol', 'fiscal_date_ending', 'period_type', name='_alpha_cashflow_uc'),)

    def __repr__(self):
        return f"<AlphaVantageCashFlowsRaw(symbol='{self.symbol}', fiscal_date_ending='{self.fiscal_date_ending}', period_type='{self.period_type}')>"

# 5. dim_companies (기업 정보 차원 테이블)
class DimCompany(Base):
    __tablename__ = 'dim_companies'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), Unique=True, nullable=False) # 종목 코드는 유니크
    company_name = Column(String(255)) # Name 필드를 company_name으로
    asset_type = Column(String(50))
    description = Column(Text)
    exchange = Column(String(50))
    currency = Column(String(10))
    country = Column(String(50))
    sector = Column(String(100))
    industry = Column(String(100))
    market_capitalization = Column(BigInteger)
    pe_ratio = Column(Numeric)
    dividend_yield = Column(Numeric)
    # fmp_id, isin 등 추가적인 기업 메타데이터 필드들은 여기에 추가 가능
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow) # 마지막 업데이트 시간 기록

    def __repr__(self):
        return f"<DimCompany(symbol='{self.symbol}', company_name='{self.company_name}')>"

# --- 데이터 수집 및 저장 함수 (새로운 스키마 및 SQLAlchemy 기반으로 수정) ---

def parse_numeric_or_none(value):
    """문자열 값을 숫자(float/int)로 파싱하거나, 유효하지 않으면 None을 반환합니다."""
    s_value = str(value).strip().lower()
    if not s_value or s_value in ['none', 'null', 'nan']:
        return None
    try:
        if '.' in s_value:
            return float(s_value)
        else:
            return int(s_value)
    except (ValueError, TypeError):
        return None

def collect_and_save_daily_ohlcv_alphavantage(symbol, api_key, outputsize='full'):
    logger.info(f"[{symbol}] AlphaVantage에서 주식 일별 OHLCV 데이터를 수집 중입니다...")

    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize={outputsize}&apikey={api_key}"

    session = None
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Error Message" in data:
            logger.error(f"[{symbol}] AlphaVantage API 오류: {data['Error Message']}")
            return
        if "Note" in data:
            logger.warning(f"[{symbol}] AlphaVantage API 참고: {data['Note']}")
            return
        if "Time Series (Daily)" not in data:
            logger.warning(f"[{symbol}] AlphaVantage에서 주식 데이터를 찾을 수 없습니다. 응답: {data}")
            return

        time_series = data["Time Series (Daily)"]

        ohlcv_db_objects = []
        ohlcv_file_records = []

        # CSV 저장을 위한 컬럼 순서는 DB 모델의 컬럼 순서와 동일하게 정의
        ohlcv_columns_for_csv = [
            'symbol', 'date', 'open', 'high', 'low', 'close', 'volume'
        ]

        for date_str, values in time_series.items():
            try:
                trade_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                open_price = parse_numeric_or_none(values['1. open'])
                high_price = parse_numeric_or_none(values['2. high'])
                low_price = parse_numeric_or_none(values['3. low'])
                close_price = parse_numeric_or_none(values['4. close'])
                volume = parse_numeric_or_none(values['5. volume'])

                # SQLAlchemy 객체 생성
                ohlcv_obj = AlphaVantageDailyOHLCVRaw(
                    symbol=symbol,
                    date=trade_date,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume
                )
                ohlcv_db_objects.append(ohlcv_obj)

                # CSV 파일 레코드 (DB 모델 컬럼 순서와 동일하게 구성)
                ohlcv_file_records.append({
                    'symbol': symbol,
                    'date': trade_date,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': volume
                })
            except (ValueError, TypeError) as ve:
                logger.error(f"[{symbol}] {date_str} 데이터 변환 오류: {ve}. 데이터: {values}")
                continue
            except Exception as e:
                logger.error(f"[{symbol}] {date_str} 주식 데이터 처리 중 오류 발생: {e}", exc_info=True)
                continue

        # 데이터베이스에 저장 (SQLAlchemy ORM)
        if ohlcv_db_objects:
            session = get_db_session()
            try:
                db_insert_count = 0
                for obj in ohlcv_db_objects:
                    # ON CONFLICT (symbol, date) DO UPDATE 로직
                    stmt = pg_insert(AlphaVantageDailyOHLCVRaw).values(
                        symbol=obj.symbol,
                        date=obj.date,
                        open=obj.open,
                        high=obj.high,
                        low=obj.low,
                        close=obj.close,
                        volume=obj.volume
                    )
                    on_conflict_stmt = stmt.on_conflict_do_update(
                        index_elements=[AlphaVantageDailyOHLCVRaw.symbol, AlphaVantageDailyOHLCVRaw.date],
                        set_=dict(
                            open=stmt.excluded.open,
                            high=stmt.excluded.high,
                            low=stmt.excluded.low,
                            close=stmt.excluded.close,
                            volume=stmt.excluded.volume
                        )
                    )
                    session.execute(on_conflict_stmt)
                    db_insert_count += 1
                session.commit()
                logger.info(f"[{symbol}] {db_insert_count}개의 AlphaVantage OHLCV 레코드를 데이터베이스에 성공적으로 저장/업데이트했습니다.")
            except Exception as e:
                logger.error(f"[{symbol}] OHLCV 데이터를 데이터베이스에 저장하는 중 오류 발생: {e}", exc_info=True)
                session.rollback()
            finally:
                if session:
                    session.close()
        else:
            logger.warning(f"[{symbol}] 수집된 OHLCV 데이터가 없습니다; 데이터베이스에 저장된 내용이 없습니다.")

        # CSV 파일에 저장
        if ohlcv_file_records:
            df = pd.DataFrame(ohlcv_file_records, columns=ohlcv_columns_for_csv)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values(by='date').reset_index(drop=True)

            # dim_companies 테이블에서 기업 정보 조회
            company_info = get_company_info_from_db(symbol) # 이 함수도 SQLAlchemy 기반으로 수정됨

            # 폴더명에 특수문자 제거
            exchange_name = company_info.get('exchange', 'Unknown_Exchange').replace('/', '_').replace('\\', '_')
            industry_name = company_info.get('industry', 'Unknown_Industry').replace('/', '_').replace('\\', '_')

            target_folder = os.path.join(ALPHA_VANTAGE_DATA_FOLDER, exchange_name, industry_name, "ohlcv")
            ensure_data_folder_exists(target_folder)

            file_path = os.path.join(target_folder, f"{symbol}_ohlcv.csv")
            df.to_csv(file_path, index=False)
            logger.info(f"[{symbol}] {len(df)}개의 AlphaVantage OHLCV 레코드를 '{file_path}'에 성공적으로 저장했습니다.")
        else:
            logger.warning(f"[{symbol}] 수집된 OHLCV 데이터가 없습니다; CSV 파일에 저장된 내용이 없습니다.")

    except requests.exceptions.RequestException as e:
        logger.error(f"[{symbol}] AlphaVantage OHLCV API 요청 오류: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[{symbol}] AlphaVantage OHLCV 데이터 수집 중 예기치 않은 오류 발생: {e}", exc_info=True)
    finally:
        if session:
            session.close()


def collect_and_save_financials_alphavantage(symbol, api_key):
    logger.info(f"[{symbol}] AlphaVantage에서 재무제표 데이터 수집을 시작합니다...")

    if not api_key:
        logger.warning(f"[{symbol}] AlphaVantage API 키를 찾을 수 없습니다. 재무제표 수집을 건너뜜.")
        return

    session = None
    try:
        urls = {
            'income': f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey={api_key}",
            'balance': f"https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={symbol}&apikey={api_key}",
            'cashflow': f"https://www.alphavantage.co/query?function=CASH_FLOW&symbol={symbol}&apikey={api_key}"
        }

        # 각 재무제표 타입별로 DB 모델과 CSV 컬럼 정의
        financial_configs = {
            'income': {
                'model': AlphaVantageIncomeStatementsRaw,
                'csv_cols': [
                    'symbol', 'fiscal_date_ending', 'reported_currency', 'reported_date', 'period_type',
                    'gross_profit', 'total_revenue', 'cost_of_revenue', 'operating_income', 'operating_expenses',
                    'selling_general_and_administrative', 'research_and_development', 'depreciation_and_amortization',
                    'income_before_tax', 'net_income', 'ebitda', 'eps'
                ],
                'db_fields': { # API 응답 필드와 DB 모델 필드 매핑
                    'fiscalDateEnding': 'fiscal_date_ending',
                    'reportedCurrency': 'reported_currency',
                    'publishedDate': 'reported_date', # API에서 'reportedDate' 대신 'publishedDate' 사용 가능성
                    'grossProfit': 'gross_profit',
                    'totalRevenue': 'total_revenue',
                    'costOfRevenue': 'cost_of_revenue',
                    'operatingIncome': 'operating_income',
                    'operatingExpenses': 'operating_expenses',
                    'sellingGeneralAndAdministrative': 'selling_general_and_administrative',
                    'researchAndDevelopment': 'research_and_development',
                    'depreciationAndAmortization': 'depreciation_and_amortization',
                    'incomeBeforeTax': 'income_before_tax',
                    'netIncome': 'net_income',
                    'ebitda': 'ebitda',
                    'eps': 'eps' # AlphaVantage Income Statement API에 EPS 필드가 명시적으로 없을 수 있음 (Earnings API에 있음)
                                # 없으면 None으로 저장될 것이므로 괜찮음.
                }
            },
            'balance': {
                'model': AlphaVantageBalanceSheetsRaw,
                'csv_cols': [
                    'symbol', 'fiscal_date_ending', 'reported_currency', 'reported_date', 'period_type',
                    'total_assets', 'current_assets', 'cash_and_cash_equivalents', 'net_receivables', 'inventory',
                    'total_non_current_assets', 'property_plant_and_equipment', 'intangible_assets', 'total_liabilities',
                    'current_liabilities', 'current_accounts_payable', 'short_term_debt', 'total_non_current_liabilities',
                    'long_term_debt', 'total_shareholder_equity', 'retained_earnings', 'common_stock'
                ],
                'db_fields': {
                    'fiscalDateEnding': 'fiscal_date_ending',
                    'reportedCurrency': 'reported_currency',
                    'publishedDate': 'reported_date',
                    'totalAssets': 'total_assets',
                    'currentAssets': 'current_assets',
                    'cashAndCashEquivalents': 'cash_and_cash_equivalents',
                    'netReceivables': 'net_receivables',
                    'inventory': 'inventory',
                    'totalNonCurrentAssets': 'total_non_current_assets',
                    'propertyPlantAndEquipment': 'property_plant_and_equipment',
                    'intangibleAssets': 'intangible_assets',
                    'totalLiabilities': 'total_liabilities',
                    'currentLiabilities': 'current_liabilities',
                    'currentAccountsPayable': 'current_accounts_payable',
                    'shortTermDebt': 'short_term_debt',
                    'totalNonCurrentLiabilities': 'total_non_current_liabilities',
                    'longTermDebt': 'long_term_debt',
                    'totalShareholderEquity': 'total_shareholder_equity',
                    'retainedEarnings': 'retained_earnings',
                    'commonStock': 'common_stock'
                }
            },
            'cashflow': {
                'model': AlphaVantageCashFlowsRaw,
                'csv_cols': [
                    'symbol', 'fiscal_date_ending', 'reported_currency', 'reported_date', 'period_type',
                    'operating_cashflow', 'payments_for_operating_activities', 'proceeds_from_operating_activities',
                    'change_in_operating_liabilities', 'change_in_operating_assets', 'depreciation_depletion_and_amortization',
                    'capital_expenditures', 'investments_cashflow', 'dividends_paid', 'net_borrowings',
                    'other_cash_flow_from_financing_activities', 'free_cash_flow'
                ],
                'db_fields': {
                    'fiscalDateEnding': 'fiscal_date_ending',
                    'reportedCurrency': 'reported_currency',
                    'publishedDate': 'reported_date',
                    'operatingCashflow': 'operating_cashflow',
                    'paymentsForOperatingActivities': 'payments_for_operating_activities',
                    'proceedsFromOperatingActivities': 'proceeds_from_operating_activities',
                    'changeInOperatingLiabilities': 'change_in_operating_liabilities',
                    'changeInOperatingAssets': 'change_in_operating_assets',
                    'depreciationDepletionAndAmortization': 'depreciation_depletion_and_amortization',
                    'capitalExpenditures': 'capital_expenditures',
                    'investmentsCashflow': 'investments_cashflow',
                    'dividendsPaid': 'dividends_paid',
                    'netBorrowings': 'net_borrowings',
                    'otherCashflowFromFinancingActivities': 'other_cash_flow_from_financing_activities',
                    'freeCashFlow': 'free_cash_flow' # Free Cash Flow는 계산된 값일 수도 있음. API에서 직접 제공하는지 확인 필요
                }
            }
        }

        for stmt_type, url in urls.items():
            config = financial_configs[stmt_type]
            Model = config['model']
            csv_columns = config['csv_cols']
            db_field_map = config['db_fields']

            logger.info(f"[{symbol}] AlphaVantage {stmt_type.upper()} 데이터 수집 중...")

            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "Error Message" in data:
                logger.error(f"[{symbol}] AlphaVantage {stmt_type} API 오류: {data['Error Message']}")
                continue
            if "Note" in data:
                logger.warning(f"[{symbol}] AlphaVantage {stmt_type} API 참고: {data['Note']}")
                continue

            reports_list = []
            if 'quarterlyReports' in data:
                reports_list.extend([{'report': r, 'period_type': 'quarterly'} for r in data['quarterlyReports']])
            if 'annualReports' in data:
                reports_list.extend([{'report': r, 'period_type': 'annual'} for r in data['annualReports']])

            if not reports_list:
                logger.warning(f"[{symbol}] AlphaVantage {stmt_type}에 대한 분기/연간 보고서를 찾을 수 없습니다. 응답: {data}")
                continue

            db_objects = []
            file_records = []

            for entry in reports_list:
                report = entry['report']
                period_type = entry['period_type']
                fiscal_date_ending_str = report.get('fiscalDateEnding')

                if not fiscal_date_ending_str:
                    logger.warning(f"[{symbol}] {stmt_type} 보고서에서 fiscalDateEnding을 찾을 수 없습니다. 스킵합니다: {report}")
                    continue

                try:
                    fiscal_date_ending = datetime.strptime(fiscal_date_ending_str, '%Y-%m-%d').date()
                    reported_date = parse_numeric_or_none(report.get('reportedDate')) # 날짜 필드이므로 float/int 파싱 함수 사용 X
                    if reported_date:
                        reported_date = datetime.strptime(reported_date, '%Y-%m-%d').date()
                    else:
                        reported_date = None # reportedDate 필드가 없을 경우

                    # DB 모델 객체에 저장할 데이터 딕셔너리
                    db_data = {
                        'symbol': symbol,
                        'fiscal_date_ending': fiscal_date_ending,
                        'reported_currency': report.get('reportedCurrency'),
                        'reported_date': reported_date,
                        'period_type': period_type,
                    }
                    # API 응답 필드 -> DB 모델 필드 매핑하여 값 추가
                    for api_field, db_col in db_field_map.items():
                        if api_field in report:
                            # 'reportedDate'는 이미 처리했으므로 건너뛰기
                            if db_col == 'reported_date':
                                continue
                            db_data[db_col] = parse_numeric_or_none(report[api_field])
                        else:
                            # API 응답에 없는 필드는 None으로 처리
                            db_data[db_col] = None
                    
                    db_obj = Model(**db_data)
                    db_objects.append(db_obj)

                    # CSV 파일 레코드 생성 (csv_columns 순서에 맞춰)
                    csv_record = {'symbol': symbol, 'fiscal_date_ending': fiscal_date_ending, 'period_type': period_type}
                    # 모든 DB 컬럼을 CSV 레코드에 추가하되, 없는 경우 None
                    for col in csv_columns:
                        if col in db_data: # 이미 db_data에 있는 필드는 그대로 사용
                            csv_record[col] = db_data[col]
                        elif hasattr(db_obj, col): # db_obj에 있는 필드는 가져오기 (예: autoincrement id 등은 제외)
                             csv_record[col] = getattr(db_obj, col) # 실제 ID는 DB 저장 후에 알 수 있으므로, CSV에는 포함하지 않음
                        else:
                            # DB 모델에 없거나 API 응답에 없는 필드는 None으로 채움
                            # 특히 'id' 컬럼은 CSV에 저장할 때 보통 제외하므로, 이 부분은 유연하게 처리
                            if col != 'id': # id 컬럼은 CSV에 저장하지 않음 (DB에서 자동 생성)
                                if col not in csv_record:
                                    csv_record[col] = None

                    # 최종 CSV 레코드를 순서에 맞춰서 구성
                    ordered_csv_record = {col: csv_record.get(col) for col in csv_columns}
                    file_records.append(ordered_csv_record)

                except (ValueError, TypeError) as ve:
                    logger.error(f"[{symbol}] {stmt_type} {fiscal_date_ending_str} 데이터 변환 오류: {ve}. 데이터: {report}")
                    continue
                except Exception as e:
                    logger.error(f"[{symbol}] {stmt_type} {fiscal_date_ending_str} 데이터 처리 중 오류 발생: {e}", exc_info=True)
                    continue
            
            # 데이터베이스에 저장 (SQLAlchemy ORM)
            if db_objects:
                session = get_db_session()
                try:
                    db_insert_count = 0
                    for obj in db_objects:
                        # ON CONFLICT DO UPDATE 로직 (PK: symbol, fiscal_date_ending, period_type)
                        # 모든 컬럼을 업데이트하는 방식으로 (id 제외)
                        stmt = pg_insert(Model).values(**obj.__dict__) # __dict__에서 _sa_instance_state 제거
                        # __dict__에서 불필요한 SQLAlchemy 내부 필드 제거
                        values_to_insert = {k: v for k, v in obj.__dict__.items() if not k.startswith('_sa_')}

                        stmt = pg_insert(Model).values(**values_to_insert)

                        # PK에 따라 on_conflict_do_update의 index_elements 설정
                        index_elements = [Model.symbol, Model.fiscal_date_ending, Model.period_type]
                        
                        # 업데이트할 컬럼들을 동적으로 구성 (id, primary key, updated_at 제외)
                        # API에서 오는 모든 필드를 업데이트 대상으로 포함
                        update_cols = {
                            k: stmt.excluded.get(k) for k in values_to_insert.keys()
                            if k not in [col.key for col in Model.__table__.primary_key.columns] and k != 'updated_at'
                        }
                        # updated_at 컬럼이 있다면, onupdate 설정을 따르게 함 (여기서는 models에 이미 설정되어 있음)

                        on_conflict_stmt = stmt.on_conflict_do_update(
                            index_elements=index_elements,
                            set_=update_cols
                        )
                        session.execute(on_conflict_stmt)
                        db_insert_count += 1
                    session.commit()
                    logger.info(f"[{symbol}] {db_insert_count}개의 AlphaVantage {stmt_type.upper()} 레코드를 데이터베이스에 성공적으로 저장/업데이트했습니다.")
                except Exception as e:
                    logger.error(f"[{symbol}] {stmt_type.upper()} 데이터를 데이터베이스에 저장하는 중 오류 발생: {e}", exc_info=True)
                    session.rollback()
                finally:
                    if session:
                        session.close()
            else:
                logger.warning(f"[{symbol}] 수집된 {stmt_type.upper()} 데이터가 없습니다; 데이터베이스에 저장된 내용이 없습니다.")

            # CSV 파일에 저장
            if file_records:
                df = pd.DataFrame(file_records, columns=csv_columns)
                df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])
                df = df.sort_values(by='fiscal_date_ending').reset_index(drop=True)

                company_info = get_company_info_from_db(symbol) # SQLAlchemy 기반으로 수정된 함수

                exchange_name = company_info.get('exchange', 'Unknown_Exchange').replace('/', '_').replace('\\', '_')
                industry_name = company_info.get('industry', 'Unknown_Industry').replace('/', '_').replace('\\', '_')

                target_folder = os.path.join(ALPHA_VANTAGE_DATA_FOLDER, exchange_name, industry_name, stmt_type)
                ensure_data_folder_exists(target_folder)

                file_path = os.path.join(target_folder, f"{symbol}_{stmt_type}.csv")
                df.to_csv(file_path, index=False)
                logger.info(f"[{symbol}] {len(df)}개의 AlphaVantage {stmt_type.upper()} 레코드를 '{file_path}'에 성공적으로 저장했습니다.")
            else:
                logger.warning(f"[{symbol}] 수집된 {stmt_type.upper()} 데이터가 없습니다; CSV 파일에 저장된 내용이 없습니다.")
            
            time.sleep(15) # API 호출 간 지연

    except requests.exceptions.RequestException as e:
        logger.error(f"[{symbol}] AlphaVantage Financials API 요청 오류: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[{symbol}] 재무제표 데이터 수집 중 예기치 않은 오류 발생: {e}", exc_info=True)
    finally:
        if session:
            session.close()


def collect_dim_company_alphavantage(symbol, api_key):
    logger.info(f"[{symbol}] AlphaVantage에서 기업 개요 데이터 수집을 시작합니다...")

    if not api_key:
        logger.warning(f"[{symbol}] AlphaVantage API 키를 찾을 수 없습니다. 기업 개요 수집을 건너뜁니다.")
        return {}

    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={api_key}"

    session = None
    company_data = {}
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Error Message" in data:
            logger.error(f"[{symbol}] AlphaVantage 기업 개요 API 오류: {data['Error Message']}")
            return {}
        if not data or len(data) == 0:
            logger.warning(f"[{symbol}] AlphaVantage에서 기업 개요 데이터를 찾을 수 없습니다. 응답: {data}")
            return {}

        company_data = {
            'symbol': symbol,
            'asset_type': data.get('AssetType'),
            'company_name': data.get('Name'), # 'Name' -> 'company_name'
            'description': data.get('Description'),
            'exchange': data.get('Exchange'),
            'currency': data.get('Currency'),
            'country': data.get('Country'),
            'sector': data.get('Sector'),
            'industry': data.get('Industry'),
            'market_capitalization': parse_numeric_or_none(data.get('MarketCapitalization')),
            'pe_ratio': parse_numeric_or_none(data.get('PERatio')),
            'dividend_yield': parse_numeric_or_none(data.get('DividendYield'))
            # 'id'는 DB에서 자동 생성
        }

        # 데이터베이스에 저장 (SQLAlchemy ORM)
        session = get_db_session()
        try:
            # DimCompany는 PRIMARY KEY가 id이고, symbol은 Unique 제약 조건
            # merge를 사용하여 symbol 기준으로 upsert 처리
            # updated_at은 모델에서 default=datetime.utcnow, onupdate=datetime.utcnow로 설정되어 있으므로 별도 처리 불필요
            
            # 먼저 symbol로 기존 레코드 조회
            existing_company = session.query(DimCompany).filter_by(symbol=symbol).first()
            
            if existing_company:
                # 기존 레코드 업데이트
                for key, value in company_data.items():
                    if key != 'symbol': # symbol은 PK 또는 Unique key이므로 업데이트 대상에서 제외
                        setattr(existing_company, key, value)
                logger.info(f"[{symbol}] 기존 기업 개요 데이터를 업데이트했습니다.")
            else:
                # 새 레코드 삽입
                new_company = DimCompany(**company_data)
                session.add(new_company)
                logger.info(f"[{symbol}] 새로운 기업 개요 데이터를 삽입했습니다.")

            session.commit()
            logger.info(f"[{symbol}] 기업 개요 데이터를 데이터베이스에 성공적으로 저장/업데이트했습니다.")
        except Exception as e:
            logger.error(f"[{symbol}] 기업 개요 데이터를 데이터베이스에 저장하는 중 오류 발생: {e}", exc_info=True)
            session.rollback()
        finally:
            if session:
                session.close()

        # CSV 파일에 저장
        if company_data:
            # CSV 저장을 위한 컬럼 순서는 DB 모델의 컬럼 순서와 동일하게 정의
            dim_company_columns_for_csv = [
                'symbol', 'company_name', 'asset_type', 'description', 'exchange', 'currency',
                'country', 'sector', 'industry', 'market_capitalization',
                'pe_ratio', 'dividend_yield'
            ]
            df = pd.DataFrame([company_data], columns=dim_company_columns_for_csv)

            target_folder = os.path.join(ALPHA_VANTAGE_DATA_FOLDER, "info")
            ensure_data_folder_exists(target_folder)

            file_path = os.path.join(target_folder, f"{symbol}_company_info.csv")
            df.to_csv(file_path, index=False)
            logger.info(f"[{symbol}] 기업 개요 데이터를 '{file_path}'에 성공적으로 저장했습니다.")
        else:
            logger.warning(f"[{symbol}] 기업 개요 데이터가 없어 CSV 파일에 저장된 내용이 없습니다.")

        return company_data

    except requests.exceptions.RequestException as e:
        logger.error(f"[{symbol}] AlphaVantage 기업 개요 API 요청 오류: {e}", exc_info=True)
        return {}
    except Exception as e:
        logger.error(f"[{symbol}] 기업 개요 데이터 수집 중 예기치 않은 오류 발생: {e}", exc_info=True)
        return {}
    finally:
        if session:
            session.close()

# DB에서 기업 정보 조회 함수 (수정됨)
def get_company_info_from_db(symbol):
    """데이터베이스에서 특정 기업의 정보를 조회합니다."""
    session = None
    try:
        session = get_db_session()
        company_obj = session.query(DimCompany).filter_by(symbol=symbol).first()
        if company_obj:
            # SQLAlchemy 객체를 딕셔너리로 변환하여 반환
            return {c.name: getattr(company_obj, c.name) for c in company_obj.__table__.columns}
        else:
            return {}
    except Exception as e:
        logger.error(f"[{symbol}] DB에서 기업 정보 조회 중 오류 발생: {e}", exc_info=True)
        return {}
    finally:
        if session:
            session.close()

# --- 메인 실행 로직 (예시) ---
if __name__ == "__main__":
    # 데이터베이스 테이블 생성 (스크립트 시작 시 한 번만 호출)
    create_tables()

    # AlphaVantage API 키 설정
    ALPHA_VANTAGE_API_KEY = config_loader.CONFIG.get('api_keys', {}).get('ALPHA_VANTAGE_API_KEY')
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("config.py에 ALPHA_VANTAGE_API_KEY가 설정되지 않았습니다.")
        sys.exit(1)

    test_symbols = ['AAPL', 'MSFT'] # 테스트할 종목 리스트

    for symbol in test_symbols:
        logger.info(f"\n--- {symbol} 데이터 수집 시작 ---")
        
        # 1. 기업 정보 수집 및 저장 (가장 먼저 수행하여 CSV 경로에 사용될 정보 확보)
        collect_dim_company_alphavantage(symbol, ALPHA_VANTAGE_API_KEY)
        time.sleep(15) # API 요청 제한 준수

        # 2. OHLCV 데이터 수집 및 저장
        collect_and_save_daily_ohlcv_alphavantage(symbol, ALPHA_VANTAGE_API_KEY)
        time.sleep(15) # API 요청 제한 준수

        # 3. 재무제표 데이터 수집 및 저장 (손익계산서, 재무상태표, 현금흐름표)
        collect_and_save_financials_alphavantage(symbol, ALPHA_VANTAGE_API_KEY)
        time.sleep(15) # API 요청 제한 준수 (총 3번 호출되므로 주의)
        
        logger.info(f"--- {symbol} 데이터 수집 완료 ---\n")

    logger.info("모든 AlphaVantage 데이터 수집 및 저장 프로세스 완료.")