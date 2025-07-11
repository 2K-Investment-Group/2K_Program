import yaml
import os
import logging
from dotenv import load_dotenv # .env 파일을 로드하기 위한 라이브러리 추가
import sys # sys 모듈 추가 (sys.exit를 위해)

# .env 파일 로드 (스크립트 시작 시 가장 먼저 실행)
load_dotenv()

# config_loader를 위한 로깅 설정
logger = logging.getLogger(__name__)

# 전역 CONFIG 변수는 load_config 함수가 호출된 후 채워지도록 합니다.
CONFIG = {}

def load_config(config_path='config.yaml'):
    """
    config.yaml 파일을 로드하고 환경 변수에서 민감한 정보를 가져와 설정합니다.
    Args:
        config_path (str): config.yaml 파일의 경로.
    Returns:
        dict: 로드되고 처리된 설정 딕셔너리.
    Raises:
        FileNotFoundError: config.yaml 파일을 찾을 수 없을 때.
        yaml.YAMLError: config.yaml 파일 파싱 오류가 발생할 때.
        ValueError: 필수 환경 변수를 찾을 수 없을 때.
        Exception: 기타 예상치 못한 오류가 발생할 때.
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f: # 인코딩 추가
            raw_config = yaml.safe_load(f) or {} # 빈 파일일 경우 대비

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
                    logger.info(f"데이터베이스 '{db_key[:-4]}'를 환경 변수 '{env_var_name}'에서 로드했습니다.")
                else:
                    logger.error(f"환경 변수 '{env_var_name}' (데이터베이스 {db_key[:-4]})를 찾을 수 없습니다. 설정했는지 확인하세요.")
                    # 필요한 경우 여기서 프로그램 종료 (치명적인 오류로 간주)
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
                logger.warning(f"API 키 '{key_name}'에 대한 환경 변수 '{env_var_name}'를 찾을 수 없습니다. 해당 API 호출은 건너뛸 수 있습니다.")
                processed_api_keys[key_name] = None # 값이 없으면 None으로 설정
        processed_config['api_keys'] = processed_api_keys

        # 기타 설정은 직접 복사 (처리되지 않은 최상위 키들)
        for key, value in raw_config.items():
            if key not in ['database', 'api_keys']: # 이미 처리된 섹션은 제외
                processed_config[key] = value
        
        return processed_config

    except FileNotFoundError:
        logger.critical(f"config.yaml 파일을 '{config_path}' 경로에서 찾을 수 없습니다. 프로젝트 루트 디렉터리에 생성해주세요.")
        raise # 예외를 다시 발생시켜 메인 스크립트에서 처리하도록 함
    except yaml.YAMLError as exc:
        logger.critical(f"config.yaml 파일 파싱 오류: {exc}")
        raise # YAML 파싱 오류 발생 시 예외 발생
    except Exception as e:
        logger.critical(f"config_loader에서 예상치 못한 오류가 발생했습니다: {e}", exc_info=True) # exc_info=True 추가
        raise # 기타 예상치 못한 오류 발생 시 예외 발생

# 스크립트가 import될 때 자동으로 설정을 로드
try:
    CONFIG.update(load_config()) # 전역 CONFIG 딕셔너리를 업데이트
except Exception as e:
    # 이 부분의 로깅은 config_loader 자체의 로깅이 아닌, import하는 파일의 로깅 시스템이
    # 제대로 설정되기 전에 발생할 수 있으므로, stderr로 직접 출력하는 것이 더 안전할 수 있습니다.
    # 하지만 현재 로깅 설정이 config_loader.py에도 되어 있으므로 유지합니다.
    logger.critical(f"설정 파일 로드 중 심각한 오류가 발생하여 프로그램이 종료됩니다: {e}")
    sys.exit(1) # 설정 로드 실패 시 프로그램 강제 종료