import asyncio
import logging
import yfinance as yf
from datetime import date, datetime
from sqlalchemy.orm import Session
from sqlalchemy import text
from src.shared.config.app_settings import get_settings
from src.database.db_connection import SessionLocal, get_db
from src.shared.exceptions.exceptions import DataCollectionError
from src.shared.logger.logger import logger

class YfinanceCollector:
    """
    yfinance 라이브러리를 사용하여 주식 가격, 옵션, 배당금 데이터를 수집하고
    TimescaleDB에 저장하는 클래스입니다.
    """
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.settings = get_settings()

    async def _fetch_and_save_stock_price(self, symbol: str):
        """특정 주식의 현재 가격을 가져와 stock_prices 테이블에 저장합니다."""
        try:
            ticker = yf.Ticker(symbol)
            # `history`를 사용하여 정확한 타임스탬프와 가격을 가져옵니다.
            df = ticker.history(period="1d")
            if not df.empty:
                price = df['Close'].iloc[-1]
                time = df.index[-1].to_pydatetime()
                
                insert_stmt = text(
                    "INSERT INTO stock_prices (time, symbol, price) VALUES (:time, :symbol, :price) "
                    "ON CONFLICT (time, symbol) DO NOTHING"
                )
                self.db_session.execute(insert_stmt, {'time': time, 'symbol': symbol, 'price': price})
                self.db_session.commit()
                logger.info(f"'{symbol}'의 현재 가격 데이터가 성공적으로 저장되었습니다.")
            else:
                logger.warning(f"'{symbol}'에 대한 주가 데이터를 가져오지 못했습니다.")
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"'{symbol}'의 주가 데이터 수집 중 오류 발생: {e}", exc_info=True)
            raise DataCollectionError(f"'{symbol}' 주가 수집 실패: {e}") from e

    async def _fetch_and_save_dividends(self, symbol: str):
        """특정 주식의 배당금 기록을 가져와 dividends 테이블에 저장합니다."""
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.dividends
            if not df.empty:
                dividends_to_insert = []
                for record_date, amount in df.items():
                    dividends_to_insert.append({
                        'symbol': symbol,
                        'date': record_date.to_pydatetime().date(),
                        'amount': amount
                    })
                
                insert_stmt = text(
                    "INSERT INTO dividends (symbol, date, amount) VALUES (:symbol, :date, :amount) "
                    "ON CONFLICT (symbol, date) DO NOTHING"
                )
                self.db_session.execute(insert_stmt, dividends_to_insert)
                self.db_session.commit()
                logger.info(f"'{symbol}'의 배당금 기록이 성공적으로 저장되었습니다.")
            else:
                logger.warning(f"'{symbol}'에 대한 배당금 데이터를 가져오지 못했습니다.")
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"'{symbol}'의 배당금 데이터 수집 중 오류 발생: {e}", exc_info=True)
            raise DataCollectionError(f"'{symbol}' 배당금 수집 실패: {e}") from e

    async def _fetch_and_save_option_chain(self, symbol: str):
        """
        특정 주식의 옵션 체인을 가져와 option_chains 테이블에 저장합니다.
        내재 변동성(Implied Volatility)을 포함합니다.
        """
        try:
            ticker = yf.Ticker(symbol)
            # 만기일 목록 가져오기
            expirations = ticker.options
            if not expirations:
                logger.warning(f"'{symbol}'에 대한 옵션 만기일이 없습니다.")
                return

            options_to_insert = []
            for expiration_date_str in expirations:
                # 옵션 체인 가져오기 (만기일별)
                option_chain = ticker.option_chain(expiration_date_str)
                expiration_date = datetime.strptime(expiration_date_str, '%Y-%m-%d').date()
                
                # 콜 옵션 데이터 처리
                for index, row in option_chain.calls.iterrows():
                    options_to_insert.append({
                        'time': datetime.now(),
                        'symbol': symbol,
                        'expiration_date': expiration_date,
                        'strike': row['strike'],
                        'option_type': 'call',
                        'implied_volatility': row['impliedVolatility']
                    })
                
                # 풋 옵션 데이터 처리
                for index, row in option_chain.puts.iterrows():
                    options_to_insert.append({
                        'time': datetime.now(),
                        'symbol': symbol,
                        'expiration_date': expiration_date,
                        'strike': row['strike'],
                        'option_type': 'put',
                        'implied_volatility': row['impliedVolatility']
                    })

            if options_to_insert:
                insert_stmt = text(
                    "INSERT INTO option_chains (time, symbol, expiration_date, strike, option_type, implied_volatility) "
                    "VALUES (:time, :symbol, :expiration_date, :strike, :option_type, :implied_volatility) "
                    "ON CONFLICT (time, symbol, expiration_date, strike, option_type) DO NOTHING"
                )
                self.db_session.execute(insert_stmt, options_to_insert)
                self.db_session.commit()
                logger.info(f"'{symbol}'의 옵션 체인 데이터가 성공적으로 저장되었습니다.")
            
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"'{symbol}'의 옵션 체인 데이터 수집 중 오류 발생: {e}", exc_info=True)
            raise DataCollectionError(f"'{symbol}' 옵션 체인 수집 실패: {e}") from e

    async def collect_all_data(self, symbols: list):
        """지정된 모든 주식에 대해 모든 데이터를 수집합니다."""
        logger.info(f"Yfinance 데이터 수집을 시작합니다. 대상 종목: {symbols}")
        for symbol in symbols:
            try:
                # 데이터 수집은 직렬적으로 실행하여 yfinance의 속도 제한을 준수합니다.
                await self._fetch_and_save_stock_price(symbol)
                await self._fetch_and_save_dividends(symbol)
                await self._fetch_and_save_option_chain(symbol)
                logger.info(f"'{symbol}'에 대한 모든 데이터 수집 및 저장 완료.")
                
                # 다음 요청까지 약간의 지연 시간을 둡니다.
                await asyncio.sleep(2)
                
            except DataCollectionError:
                logger.warning(f"'{symbol}'에 대한 데이터 수집 중 치명적인 오류가 발생하여 다음 종목으로 넘어갑니다.")

# --- 테스트 실행 ---
if __name__ == '__main__':
    from sqlalchemy import text
    from src.database.db_connection import get_db, test_db_connection
    from src.shared.logger.logger import logger
    
    # 데이터베이스 연결 테스트
    try:
        test_db_connection()
    except Exception as e:
        logger.error(f"데이터베이스 연결 테스트 실패. 스크립트를 종료합니다.")
        exit(1)

    # 데이터 수집 및 저장 실행
    async def main_collector():
        stock_symbols = get_settings().data_sources.stocks
        
        # FastAPI의 의존성 주입과 유사한 방식으로 DB 세션 사용
        db_session = next(get_db())
        collector = YfinanceCollector(db_session=db_session)
        
        await collector.collect_all_data(stock_symbols)

    asyncio.run(main_collector())