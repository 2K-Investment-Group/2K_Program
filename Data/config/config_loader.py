import yaml
import os
import logging
from dotenv import load_dotenv # .env 파일을 로드하기 위한 라이브러리 추가
import sys # sys 모듈 추가 (sys.exit를 위해)

# --- (1) 프로젝트 루트 경로를 동적으로 찾기 ---
current_dir = os.path.dirname(os.path.abspath(__file__))
# config_loader.py는 2K_Program/Data/config 안에 있으므로,
# PROJECT_ROOT는 현재 디렉토리에서 두 번 상위 디렉토리로 이동해야 합니다.
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

# .env 파일은 PROJECT_ROOT (2K_Program/)에 있다고 가정합니다.
DOTENV_PATH = os.path.join(PROJECT_ROOT, '.env')

# --- (2) config.yaml 파일 경로 변경: config_loader.py와 같은 디렉토리 내에 있다고 가정 ---
# config.yaml은 이제 Data/config/ 폴더 안에 있으므로,
# current_dir (Data/config/)를 기준으로 찾습니다.
CONFIG_YAML_PATH = os.path.join(current_dir, 'config.yaml')


# .env 파일 로드 (로깅이 설정되기 전에 실행되므로 print 사용)
def load_environment_variables_initial():
    """
    .env 파일에서 환경 변수를 로드합니다.
    이 함수는 로깅 시스템이 완전히 설정되기 전에 호출될 수 있으므로,
    주요 메시지는 print()를 사용합니다.
    """
    if os.path.exists(DOTENV_PATH):
        load_dotenv(DOTENV_PATH)
        print(f"INFO: '.env' 파일이 '{DOTENV_PATH}'에서 성공적으로 로드되었습니다.")
    else:
        print(f"WARNING: '.env' 파일을 '{DOTENV_PATH}' 경로에서 찾을 수 없습니다.")

# 스크립트 로드 시 .env 먼저 로드
load_environment_variables_initial()

# 로거 객체 생성 (로깅 설정은 각 Collector 스크립트의 main()에서 setup_logging을 통해 이루어짐)
logger = logging.getLogger(__name__)

# 전역 CONFIG 변수 선언. load_config 호출 후 채워집니다.
CONFIG = {}

def load_config():
    """
    config.yaml 파일을 로드하고 환경 변수에서 민감한 정보를 가져와 설정합니다.
    
    Returns:
        dict: 로드되고 처리된 설정 딕셔너리.
    
    Raises:
        FileNotFoundError: config.yaml 파일을 찾을 수 없을 때.
        yaml.YAMLError: config.yaml 파일 파싱 오류가 발생할 때.
        ValueError: 필수 환경 변수를 찾을 수 없을 때.
        Exception: 기타 예상치 못한 오류가 발생할 때.
    """
    logger.info(f"'{CONFIG_YAML_PATH}' 경로에서 config.yaml 파일 로드를 시도합니다.")
    
    if not os.path.exists(CONFIG_YAML_PATH):
        logger.critical(f"오류: config.yaml 파일을 '{CONFIG_YAML_PATH}' 경로에서 찾을 수 없습니다. '{os.path.dirname(CONFIG_YAML_PATH)}' 폴더에 생성해주세요.")
        raise FileNotFoundError(f"[Errno 2] No such file or directory: '{CONFIG_YAML_PATH}'")

    try:
        with open(CONFIG_YAML_PATH, 'r', encoding='utf-8') as f:
            raw_config = yaml.safe_load(f) or {} # 빈 파일일 경우 대비
        
        logger.info(f"'{CONFIG_YAML_PATH}' 파일이 성공적으로 로드되었습니다.")

        processed_config = {}

        # 데이터베이스 설정 처리
        db_config = raw_config.get('database', {})
        processed_db_config = {}
        for db_key, db_value in db_config.items():
            if db_key.endswith('_env'): # _env로 끝나는 키는 환경 변수를 참조
                env_var_name = str(db_value)
                env_var_value = os.getenv(env_var_name)
                if env_var_value:
                    processed_db_config[db_key[:-4]] = env_var_value # _env 제거하고 실제 키 이름으로 저장
                    logger.info(f"데이터베이스 '{db_key[:-4]}' 정보를 환경 변수 '{env_var_name}'에서 로드했습니다.")
                else:
                    logger.critical(f"오류: 환경 변수 '{env_var_name}' (데이터베이스 {db_key[:-4]})를 찾을 수 없습니다. '.env' 파일에 설정했는지 확인하세요.")
                    raise ValueError(f"필수 환경 변수 '{env_var_name}'가 설정되지 않았습니다.")
            else:
                processed_db_config[db_key] = db_value
        processed_config['database'] = processed_db_config

        # API 키 설정 처리
        api_keys_config = raw_config.get('api_keys', {})
        processed_api_keys = {}
        for key_name, value in api_keys_config.items():
            # config.yaml의 값이 환경 변수 이름이라고 가정
            env_var_name = str(value) # 값이 문자열임을 보장
            api_key_value = os.getenv(env_var_name)
            if api_key_value:
                processed_api_keys[key_name] = api_key_value
                logger.info(f"API 키 '{key_name}'를 환경 변수 '{env_var_name}'에서 로드했습니다.")
            else:
                # API 키가 필수는 아닐 수 있으므로 경고 처리. 필요하다면 여기도 critical로 변경 가능.
                logger.warning(f"경고: API 키 '{key_name}'에 대한 환경 변수 '{env_var_name}'를 찾을 수 없습니다. 해당 API 호출은 건너뛸 수 있습니다.")
                processed_api_keys[key_name] = None # 값이 없으면 None으로 설정
        processed_config['api_keys'] = processed_api_keys

        # 기타 설정은 직접 복사 (처리되지 않은 최상위 키들)
        for key, value in raw_config.items():
            if key not in ['database', 'api_keys']: # 이미 처리된 섹션은 제외
                processed_config[key] = value
        
        return processed_config

    except yaml.YAMLError as exc:
        logger.critical(f"config.yaml 파일 파싱 오류: {exc}")
        raise # YAML 파싱 오류 발생 시 예외 발생
    except Exception as e:
        logger.critical(f"설정 파일 로드 중 예상치 못한 오류가 발생했습니다: {e}", exc_info=True)
        raise # 기타 예상치 못한 오류 발생 시 예외 발생

# 스크립트가 import될 때 CONFIG 전역 변수를 로드
try:
    CONFIG.update(load_config()) # 전역 CONFIG 딕셔너리를 업데이트
except Exception as e:
    # 이 오류는 로깅 시스템이 완전히 준비되기 전에 발생할 수 있습니다.
    # 따라서 sys.stderr를 통해 직접 출력하고 프로그램을 종료합니다.
    print(f"CRITICAL ERROR: 설정 파일 로드 중 심각한 오류가 발생하여 프로그램이 종료됩니다: {e}", file=sys.stderr)
    sys.exit(1)

# 테스트를 위한 코드 (선택 사항)
if __name__ == "__main__":
    # 이 블록은 config_loader.py 파일을 직접 실행할 때만 작동합니다.
    # 테스트를 위해 간단한 로깅을 여기서 설정할 수 있습니다.
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger.info("config_loader.py를 직접 실행하여 설정 로드 테스트:")
    logger.info(f"프로젝트 루트: {PROJECT_ROOT}")
    logger.info(f".env 경로: {DOTENV_PATH}")
    logger.info(f"config.yaml 경로: {CONFIG_YAML_PATH}")
    
    if 'api_keys' in CONFIG:
        logger.info("\nAPI Keys (일부 마스킹):")
        for key, value in CONFIG['api_keys'].items():
            if value and len(str(value)) > 4: # 값의 타입을 확인하고 길이 체크
                logger.info(f"  {key}: {str(value)[:4]}...") # 앞 4자리만 보여주고 마스킹
            else:
                logger.info(f"  {key}: {value}")
    else:
        logger.warning("config.yaml에 'api_keys' 섹션이 없습니다.")

    if 'data_sources' in CONFIG:
        logger.info("\nData Sources (일부):")
        if 'fmp_symbols' in CONFIG['data_sources']:
            logger.info(f"  FMP Symbols count: {len(CONFIG['data_sources']['fmp_symbols'])}")
        if 'fred_series' in CONFIG['data_sources']:
            logger.info(f"  FRED Series count: {len(CONFIG['data_sources']['fred_series'])}")
    else:
        logger.warning("config.yaml에 'data_sources' 섹션이 없습니다.")