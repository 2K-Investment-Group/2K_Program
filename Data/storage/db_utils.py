import os
import sys
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 프로젝트 루트 디렉토리를 PYTHONPATH에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from Data.config import config_loader
from utils.logger_config import setup_logging # logger_config.py 경로가 utils 폴더에 있다면 수정 필요

logger = logging.getLogger(__name__)

def get_db_connection_string():
    db_user = config_loader.CONFIG.get('db_user')
    db_password = config_loader.CONFIG.get('db_password')
    db_name = config_loader.CONFIG.get('db_name')
    db_host = config_loader.CONFIG.get('db_host', 'localhost')
    db_port = config_loader.CONFIG.get('db_port', '5432')

    if not all([db_user, db_password, db_name]):
        logger.critical("DB 연결 정보(user, password, name)가 config.yaml에 누락되었습니다.")
        return None
    
    return f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

def get_db_engine():
    db_connection_string = get_db_connection_string()
    if not db_connection_string:
        return None
    try:
        engine = create_engine(db_connection_string, isolation_level="AUTOCOMMIT") # DDL auto-commit
        return engine
    except Exception as e:
        logger.critical(f"데이터베이스 엔진 생성 실패: {e}")
        return None

def get_db_session():
    engine = get_db_engine()
    if engine:
        Session = sessionmaker(bind=engine)
        return Session()
    return None

# db_setup.py도 이 유틸리티 함수를 사용하도록 수정합니다.
# 이 파일은 주로 다른 collector들이 DB에 데이터를 넣을 때 엔진을 생성하는 데 사용됩니다.