# Hedge Fund 자동 매매·분석 프로그램

1. 시스템 아키텍처 개요

### 전체 파이프라인

1. 데이터 수집

   * 실시간 및 과거 데이터 수집 (틱/캔들/펀더멘털/뉴스/소셜)
   * 수집 즉시 ETL 처리 및 시계열 DB에 저장

2. 데이터 처리 및 피처 엔지니어링

   * 기술적 지표, 가격/거래량 기반 파생 피처 생성
   * 펀더멘털 데이터 정규화 및 결측치 처리
   * 뉴스 및 소셜 데이터 감성 분석 후 시그널화

3. 시그널 생성

   * 룰 기반 및 ML 기반 시그널 생성 모듈
   * 전략별 트리거, 조건부 시그널, 순위 기반 시그널 출력

4. 백테스트 및 최적화

   * 전략별 과거 데이터 기반 검증 및 Walk-forward Test
   * Hyperparameter 최적화 (Grid/Random/Bayesian)

5. 자동 매매 실행

   * 전략 시그널 → 주문 Queue → Execution Engine
   * 포지션 관리, 주문 체결, 리스크 모니터링
   * 실패 및 예외 상황 핸들링, 주문 재시도 로직 포함

6. 모니터링 및 보고서 자동화

   * 실시간 포트폴리오 및 주문 현황 모니터링
   * 일/주/월간 성과 보고서 자동 생성 및 Slack/Telegram 알림

2. 데이터 수집 및 저장

### 2-1. 실시간 시세 데이터

* **캔들(OHLCV), 틱 데이터, Order Book Snapshot**

* `ccxt`, 거래소 WebSocket API 사용
* 전처리 후 TimescaleDB, InfluxDB에 시계열 형태로 저장
* 틱 데이터는 압축(Parquet/Zstd) 후 S3/Wasabi에 장기 보관

### 2-2. 펀더멘털 데이터

* EPS, PER, ROE, 부채비율 등 재무제표
* 애널리스트 컨센서스, ESG 점수
* `yfinance`, `alpha_vantage`, `financialmodelingprep` API 활용

### 2-3. 뉴스 및 소셜 데이터

* 뉴스: RSS, 크롤러, 뉴스 API(NewsAPI, Finnhub)
* 소셜: Twitter, StockTwits API 및 Selenium 크롤러
* 감성 분석 전처리를 위해 언어별 필터 및 정제 처리 후 LLM 적용

---

## 3. 데이터 처리 및 피처 엔지니어링

* 가격 기반: 이동평균, 볼린저 밴드, ATR, RSI, MACD, 스토캐스틱
* 거래량 기반: OBV, 거래량 MA, 체결강도, 호가 잔량 비율
* 펀더멘털 기반: 성장률, 밸류에이션, 수익성, 안전성 점수화
* 뉴스/소셜 기반: 긍정/부정 점수, 키워드 빈도, 이벤트 플래그
* Factor 기반: Momentum, Value, Size, Quality, Low Volatility
* 시계열 데이터 윈도우 처리, Lag 피처 생성
* 이상치 탐지 및 제거, 정규화/스케일링(0-1, Z-score)

> **구현:** `pandas`, `numpy`, `ta-lib`, `ta`, `scikit-learn` 활용

---

## 4. 시그널 생성 및 전략 모듈

### 4-1. 룰 기반 전략

* **Breakout / Trend Following**: 가격이 특정 MA 돌파 시 진입
* **Mean Reversion**: 볼린저 밴드 하단 진입 후 상단 청산
* **마켓 메이킹**: 스프레드 기반 Bid/Ask 유동성 공급
* **페어 트레이딩**: 코인/주식 쌍의 Cointegration 기반 롱숏

### 4-2. 머신러닝 기반 전략

* Supervised Learning: XGBoost, Random Forest, LightGBM
* 시계열 예측: LSTM, GRU, Transformer 기반
* AutoML 기반 모델 자동 탐색 및 성능 비교
* Feature Importance 기반 변수 선택 및 최적화

### 4-3. Factor Investing 전략

* Value, Momentum, Size, Quality 기반 포트 구성
* Risk Parity 및 Volatility Targeting을 이용한 비중 조절
* 월별/주별 리밸런싱 및 포트폴리오 업데이트

> **구현:** `scikit-learn`, `xgboost`, `lightgbm`, `pytorch`, `tensorflow`, `PyPortfolioOpt`

---

## 5. 백테스트 및 최적화

* 과거 데이터 기반 수익률, MDD, 샤프지수, 승률 계산
* 수수료, 슬리피지, 세금 반영
* Walk-forward 및 K-Fold Validation
* Hyperparameter Tuning:

  * Grid Search, Random Search, Bayesian Optimization
  * Optuna, Hyperopt 활용

> **구현:** `backtrader`, `vectorbt`, 자체 엔진 개발 가능

---

## 6. 자동 매매 실행 엔진

### 6-1. 주문 관리

* 거래소 REST API 및 WebSocket 연동
* 지정가, 시장가, 조건부 주문, 분할 체결 로직
* VWAP/TWAP 알고리즘 트레이딩
* 주문 실패 시 자동 재시도 및 상태 모니터링

### 6-2. 포지션 관리

* 각 전략 및 자산별 포지션 추적
* Stop Loss, Take Profit, Trailing Stop
* 계좌 자산 비율 기반 자동 포지션 크기 조절
* 실시간 포트폴리오 가치 및 리스크 노출 추적

### 6-3. 슬리피지 및 스프레드 대응

* Order Book 기반 최적 가격 결정
* 대량 주문 시 주문 분할
* 체결 속도 및 슬리피지 모니터링

> **구현:** `ccxt`, `ib_insync`, REST/WebSocket 직접 연결, `FastAPI`로 내부 API 구축

---

## 7. 리스크 및 자산 관리

* 실시간 PnL, MDD, Exposure 모니터링
* VaR, CVaR, Expected Shortfall 계산
* 포트폴리오 Risk Parity 적용
* 실시간 포트폴리오 리밸런싱 및 자산 헷지
* 특정 조건(예: MDD -10%) 발생 시 자동 트레이딩 중단

> **구현:** `pandas`, `numpy`, `scipy`, 자체 Risk Engine

---

## 8. 모니터링 및 자동 보고서

* 웹 기반 대시보드:

  * 실시간 가격/포트폴리오 상태
  * 각 전략별 PnL, MDD, 수익률
  * 리스크 노출 및 알림 설정

* 알림:

  * Slack, Telegram, 이메일 자동 알림
  * 이상 체결/에러 발생 시 즉시 전송

* 보고서:

  * 일간/주간/월간 성과 보고서 PDF 자동 생성
  * 전략별 수익 기여도, 거래 횟수, Win Rate, Max Drawdown 등 포함

> **구현:** `FastAPI` + `React`, `matplotlib`, `pdfkit`, `WeasyPrint`

---

## 9. 기술 스택 및 인프라

* 언어: Python (핵심 로직), Rust/C++ (초저지연 주문 처리 선택)
* DB: PostgreSQL, TimescaleDB, InfluxDB
* 서버: FastAPI (API 서버), React (대시보드)
* 배포: Docker, Kubernetes, GitHub Actions (CI/CD)
* 클라우드: AWS/Wasabi/S3 (장기 데이터 스토리지)
* 모니터링: Grafana, Prometheus

---

## 10. 개발 및 확장 로드맵

1. 데이터 수집 및 저장 파이프라인 완성
2. 전략별 시그널 생성 및 백테스트 기반 고도화
3. 실시간 주문 및 포지션 관리 엔진 구축
4. 리스크 및 자산 관리 자동화 모듈 완성
5. 웹 대시보드 및 알림 시스템 연결
6. LLM 및 ML 기반 전략 자동 탐색 고도화
7. 초저지연 주문 엔진(Rust/C++) 개발
8. 장기적으로 펀드/SMAs 운용으로 확장 가능

---

## 11. 관리 및 확장 포인트

* 데이터 품질 관리 (결측치, 이상치 처리, 일관성 검증)
* 전략 개발 → 백테스트 → 실전 투입까지 동일 코드 사용
* 계좌 별 자산 분리 및 리스크 독립성 확보
* API Key Vault 및 접근 권한 관리
* 모든 주문, 체결, 시그널, 에러 로그 DB 기록 (Audit Trail)
* Market/Limit 주문, GTC/IOC/FOK 옵션 설정 가능
* 초단타 전략 대비 Latency Profiling 및 최적화

---

## 12. 실행/설치 가이드

### 요구사항

* Python 3.11+
* Docker, Docker Compose
* PostgreSQL/TimescaleDB
* Node.js (대시보드)

### 설치

```bash
git clone https://github.com/yourname/hedgefund-trading-program.git
cd hedgefund-trading-program
docker-compose up -d
```

### 실행

* `config.yaml`에서 거래소 API Key 및 전략 파라미터 설정
* `python main.py`로 매매 엔진 실행
* `http://localhost:8000/docs`에서 API 확인
* `http://localhost:3000`에서 대시보드 접속



hedgefund-trading-program/
├── src/
│   ├── data_collection/
│   │   ├── __init__.py
│   │   ├── realtime_collector.py     # 실시간 시세 (ccxt, WebSocket)
│   │   ├── fundamental_collector.py  # 펀더멘털 (yfinance 등)
│   │   ├── news_social_collector.py  # 뉴스/소셜 (API, Selenium)
│   │   └── database.py               # TimescaleDB/InfluxDB 연결 및 저장
│   │
│   ├── data_processing/
│   │   ├── __init__.py
│   │   ├── feature_engineering.py    # 기술적 지표, 펀더멘털, 뉴스 감성 피처
│   │   ├── preprocessing.py          # 정규화, 결측치, 이상치 처리
│   │
│   ├── signal_generation/
│   │   ├── __init__.py
│   │   ├── rule_based_strategies.py  # 룰 기반 전략 (Breakout, Mean Reversion)
│   │   ├── ml_strategies.py          # ML 기반 전략 (XGBoost, LSTM)
│   │   ├── factor_strategies.py      # Factor Investing (PyPortfolioOpt)
│   │   ├── signal_manager.py         # 시그널 통합 및 관리
│   │
│   ├── backtesting/
│   │   ├── __init__.py
│   │   ├── backtest_engine.py        # backtrader/vectorbt 연동 또는 자체 엔진
│   │   ├── optimizer.py              # Optuna/Hyperopt 최적화
│   │
│   ├── trading_execution/
│   │   ├── __init__.py
│   │   ├── order_manager.py          # 주문 생성 및 관리 (ccxt)
│   │   ├── position_manager.py       # 포지션 추적, SL/TP
│   │   ├── execution_engine.py       # 주문 체결 로직, 재시도
│   │
│   ├── risk_management/
│   │   ├── __init__.py
│   │   ├── risk_monitor.py           # PnL, MDD, Exposure 모니터링
│   │   ├── asset_manager.py          # 리밸런싱, 자산 헷지
│   │
│   ├── monitoring_reporting/
│   │   ├── __init__.py
│   │   ├── dashboard_api.py          # FastAPI 대시보드 API
│   │   ├── report_generator.py       # PDF 보고서 생성
│   │   ├── notifier.py               # Slack/Telegram 알림
│   │
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── config_loader.py          # config.yaml 로드
│   │   ├── logger.py                 # 로깅 설정
│   │   ├── security.py               # API Key 관리
│   │
│   └── main.py                     # 메인 애플리케이션 진입점
│
├── config.yaml                   # 설정 파일
├── Dockerfile                    # Docker 이미지 빌드
├── docker-compose.yaml           # Docker Compose 설정
├── requirements.txt              # Python 종속성
├── README.md                     # 프로젝트 설명 (제시된 내용)
└── tests/                        # 테스트 코드




quant_db (데이터베이스)
├─── Schemas (스키마)
│    └─── public (기본 스키마. 필요시 'raw', 'transformed' 등 스키마 분리 가능)
│         ├─── Tables (테이블)
│         │    │
│         │    ├─── alpha_vantage_income_statements_raw (손익계산서 원본 데이터)
│         │    │    ├── id (PK)
│         │    │    ├── symbol (AAPL, MSFT 등)
│         │    │    ├── fiscal_date_ending (재무보고서 기준 날짜 - TimescaleDB 시간축)
│         │    │    ├── reported_currency
│         │    │    ├── reported_date
│         │    │    ├── period_type (annual, quarterly)
│         │    │    ├── gross_profit (API 필드)
│         │    │    ├── total_revenue (API 필드)
│         │    │    ├── operating_income (API 필드)
│         │    │    ├── net_income (API 필드)
│         │    │    └── ... (Alpha Vantage Income Statement의 모든 필드)
│         │    │
│         │    ├─── alpha_vantage_balance_sheets_raw (재무상태표 원본 데이터)
│         │    │    ├── id (PK)
│         │    │    ├── symbol
│         │    │    ├── fiscal_date_ending (TimescaleDB 시간축)
│         │    │    ├── reported_currency
│         │    │    ├── reported_date
│         │    │    ├── period_type
│         │    │    ├── total_assets (API 필드)
│         │    │    ├── current_assets (API 필드)
│         │    │    ├── total_liabilities (API 필드)
│         │    │    ├── total_shareholder_equity (API 필드)
│         │    │    └── ... (Alpha Vantage Balance Sheet의 모든 필드)
│         │    │
│         │    ├─── alpha_vantage_cash_flows_raw (현금흐름표 원본 데이터)
│         │    │    ├── id (PK)
│         │    │    ├── symbol
│         │    │    ├── fiscal_date_ending (TimescaleDB 시간축)
│         │    │    ├── reported_currency
│         │    │    ├── reported_date
│         │    │    ├── period_type
│         │    │    ├── operating_cashflow (API 필드)
│         │    │    ├── capital_expenditures (API 필드)
│         │    │    ├── investments_cashflow (API 필드)
│         │    │    └── ... (Alpha Vantage Cash Flow의 모든 필드)
│         │    │
│         │    ├─── fmp_historical_prices_raw (FMP 주가 원본 데이터)
│         │    │    ├── id (PK)
│         │    │    ├── symbol
│         │    │    ├── date (주가 날짜 - TimescaleDB 시간축)
│         │    │    ├── open
│         │    │    ├── high
│         │    │    ├── low
│         │    │    ├── close
│         │    │    ├── volume
│         │    │    └── ... (FMP 주가 API의 모든 필드)
│         │    │
│         │    ├─── fmp_income_statements_raw (FMP 손익계산서 원본 데이터 - FMP도 재무제표 제공)
│         │    │    ├── id (PK)
│         │    │    ├── symbol
│         │    │    ├── date (보고서 날짜 - TimescaleDB 시간축)
│         │    │    ├── reported_currency
│         │    │    ├── revenue
│         │    │    └── ... (FMP 재무제표의 모든 필드)
│         │    │
│         │    ├─── fred_series_raw (FRED 경제 지표 원본 데이터)
│         │    │    ├── id (PK)
│         │    │    ├── series_id (FRED 고유 ID, 예: 'GDP')
│         │    │    ├── date (지표 날짜 - TimescaleDB 시간축)
│         │    │    ├── value
│         │    │    └── ... (FRED API의 모든 필드)
│         │    │
│         │    ├─── world_bank_indicators_raw (월드 뱅크 지표 원본 데이터)
│         │    │    ├── id (PK)
│         │    │    ├── country_code (국가 코드, 예: 'KOR')
│         │    │    ├── indicator_code (지표 코드, 예: 'SP.POP.TOTL')
│         │    │    ├── date (지표 날짜 - TimescaleDB 시간축)
│         │    │    ├── value
│         │    │    └── ... (World Bank API의 모든 필드)
│         │    │
│         │    └─── dim_companies (차원 테이블: 기업 정보)
│         │         ├── id (PK)
│         │         ├── symbol
│         │         ├── company_name
│         │         ├── industry
│         │         ├── sector
│         │         └── ... (추가적인 기업 메타데이터)
│         │
│         └─── TimescaleDB Extensions (확장 기능)
│              └── timescaledb (시계열 데이터 처리용)




.venv\Scripts\activate

docker exec -it quant_timescaledb psql -U lucian -d quant_db
\d stock_ohlcv & \d crypto_ohlcv

