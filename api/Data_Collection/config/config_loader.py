import yaml
import os
import logging
from dotenv import load_dotenv # Import library to load .env files
import sys # Import sys module for sys.exit

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

DOTENV_PATH = os.path.join(PROJECT_ROOT, '.env')

CONFIG_YAML_PATH = os.path.join(current_dir, 'config.yaml')


def load_environment_variables_initial():
    """
    Loads environment variables from the .env file.
    This function might be called before the logging system is fully set up,
    so it uses print() for main messages.
    """
    if os.path.exists(DOTENV_PATH):
        load_dotenv(DOTENV_PATH)
        print(f"INFO: '.env' file successfully loaded from '{DOTENV_PATH}'.")
    else:
        print(f"WARNING: '.env' file not found at '{DOTENV_PATH}'.")

load_environment_variables_initial()

logger = logging.getLogger(__name__)

CONFIG = {}

def load_config():
    """
    Loads the config.yaml file and populates sensitive information from environment variables.
    
    Returns:
        dict: The loaded and processed configuration dictionary.
    
    Raises:
        FileNotFoundError: If config.yaml file is not found.
        yaml.YAMLError: If an error occurs parsing the config.yaml file.
        ValueError: If a required environment variable is not found.
        Exception: For any other unexpected errors.
    """
    logger.info(f"Attempting to load config.yaml file from '{CONFIG_YAML_PATH}'.")
    
    if not os.path.exists(CONFIG_YAML_PATH):
        logger.critical(f"Error: config.yaml file not found at '{CONFIG_YAML_PATH}'. Please create it in the '{os.path.dirname(CONFIG_YAML_PATH)}' folder.")
        raise FileNotFoundError(f"[Errno 2] No such file or directory: '{CONFIG_YAML_PATH}'")

    try:
        with open(CONFIG_YAML_PATH, 'r', encoding='utf-8') as f:
            raw_config = yaml.safe_load(f) or {} # Handle empty file case
        
        logger.info(f"Successfully loaded '{CONFIG_YAML_PATH}'.")

        processed_config = {}

        db_config = raw_config.get('database', {})
        processed_db_config = {}
        for db_key, db_value in db_config.items():
            if db_key.endswith('_env'):
                env_var_name = str(db_value)
                env_var_value = os.getenv(env_var_name)
                if env_var_value:
                    processed_db_config[db_key[:-4]] = env_var_value
                    logger.info(f"Loaded database '{db_key[:-4]}' information from environment variable '{env_var_name}'.")
                else:
                    logger.critical(f"Error: Environment variable '{env_var_name}' (database {db_key[:-4]}) not found. Ensure it's set in your '.env' file.")
                    raise ValueError(f"Required environment variable '{env_var_name}' is not set.")
            else:
                processed_db_config[db_key] = db_value
        processed_config['database'] = processed_db_config

        api_keys_config = raw_config.get('api_keys', {})
        processed_api_keys = {}
        for key_name, value in api_keys_config.items():
            env_var_name = str(value)
            api_key_value = os.getenv(env_var_name)
            if api_key_value:
                processed_api_keys[key_name] = api_key_value
                logger.info(f"Loaded API key '{key_name}' from environment variable '{env_var_name}'.")
            else:
                logger.warning(f"Warning: Environment variable '{env_var_name}' for API key '{key_name}' not found. Corresponding API calls might be skipped.")
                processed_api_keys[key_name] = None 
        processed_config['api_keys'] = processed_api_keys

        for key, value in raw_config.items():
            if key not in ['database', 'api_keys']: 
                processed_config[key] = value
        
        return processed_config

    except yaml.YAMLError as exc:
        logger.critical(f"Error parsing config.yaml file: {exc}")
        raise 
    except Exception as e:
        logger.critical(f"An unexpected error occurred while loading the configuration file: {e}", exc_info=True)
        raise
try:
    CONFIG.update(load_config()) 
except Exception as e:
    print(f"CRITICAL ERROR: A critical error occurred while loading the configuration file. Program will terminate: {e}", file=sys.stderr)
    sys.exit(1)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger.info("Testing config loading by running config_loader.py directly:")
    logger.info(f"Project Root: {PROJECT_ROOT}")
    logger.info(f".env Path: {DOTENV_PATH}")
    logger.info(f"config.yaml Path: {CONFIG_YAML_PATH}")
    
    if 'api_keys' in CONFIG:
        logger.info("\nAPI Keys (partially masked):")
        for key, value in CONFIG['api_keys'].items():
            if value and len(str(value)) > 4:
                logger.info(f"  {key}: {str(value)[:4]}...") 
            else:
                logger.info(f"  {key}: {value}")
    else:
        logger.warning("No 'api_keys' section found in config.yaml.")

    if 'data_sources' in CONFIG:
        logger.info("\nData Sources (partial):")
        if 'fmp_symbols' in CONFIG['data_sources']:
            logger.info(f"  FMP Symbols count: {len(CONFIG['data_sources']['fmp_symbols'])}")
        if 'fred_series' in CONFIG['data_sources']:
            logger.info(f"  FRED Series count: {len(CONFIG['data_sources']['fred_series'])}")
    else:
        logger.warning("No 'data_sources' section found in config.yaml.")