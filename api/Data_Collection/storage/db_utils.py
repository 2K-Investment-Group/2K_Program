import os
import sys
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 프로젝트 루트 디렉토리를 PYTHONPATH에 추가
# db_utils.py는 Data/storage 안에 있으므로,
# project_root는 현재 디렉토리에서 두 단계 위로 올라가야 '2K_Program'이 됩니다.
# current_dir (Data/storage) -> os.pardir (Data) -> os.pardir (2K_Program)
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir)) # <--- os.pardir 하나 더 추가

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 이제 Data.config.config_loader 및 utils.logger_config 임포트가 정상적으로 작동할 것입니다.
from Data.config import config_loader
from utils.logger_config import setup_logging

logger = logging.getLogger(__name__)

def get_db_connection_string():
    # config_loader.CONFIG에서 'database' 섹션을 가져옵니다.
    db_config = config_loader.CONFIG.get('database', {})

    # db_config 딕셔너리에서 각 값을 가져옵니다.
    # config_loader.py는 'password_env'를 처리하여 'password' 키로 값을 넣어줍니다.
    # config.yaml에 'dbname'으로 되어 있으므로 'dbname' 키를 사용합니다.
    db_user = db_config.get('user')
    db_password = db_config.get('password') # config_loader가 처리한 'password' 키 사용
    db_name = db_config.get('dbname')      # config.yaml의 'dbname' 키 사용
    db_host = db_config.get('host', 'localhost') # config.yaml의 'host' 키 사용
    db_port = db_config.get('port', '5432')    # config.yaml의 'port' 키 사용

    # 필수 연결 정보가 모두 있는지 확인
    if not all([db_user, db_password, db_name]):
        logger.critical(
            f"DB 연결 정보(user: {db_user}, password: {'***' if db_password else 'None'}, "
            f"name: {db_name}, host: {db_host}, port: {db_port})가 config.yaml 또는 .env 파일에 누락되었습니다."
        )
        return None
    
    return f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

def get_db_engine():
    db_connection_string = get_db_connection_string()
    if not db_connection_string:
        return None
    try:
        # 데이터베이스 타입(type)이 config.yaml에 명시되어 있다면 가져와서 사용할 수 있습니다.
        # 예: config_loader.CONFIG.get('database', {}).get('type', 'postgresql')
        # 현재는 postgresql+psycopg2로 고정되어 있으므로 그대로 둡니다.
        engine = create_engine(db_connection_string, isolation_level="AUTOCOMMIT") # DDL auto-commit
        # 연결 테스트 (실제로 연결을 시도하여 에러를 빠르게 감지)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info("데이터베이스 엔진 생성 및 연결 테스트 성공.")
        return engine
    except Exception as e:
        logger.critical(f"데이터베이스 엔진 생성 실패 또는 연결 테스트 오류: {e}")
        return None

def get_db_session():
    engine = get_db_engine()
    if engine:
        Session = sessionmaker(bind=engine)
        return Session()
    logger.critical("DB 엔진을 가져오지 못하여 세션을 생성할 수 없습니다.")
    return None

# db_setup.py도 이 유틸리티 함수를 사용하도록 수정합니다.
# 이 파일은 주로 다른 collector들이 DB에 데이터를 넣을 때 엔진을 생성하는 데 사용됩니다.