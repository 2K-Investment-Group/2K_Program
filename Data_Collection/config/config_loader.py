import yaml
import os
import logging
from dotenv import load_dotenv # Import library to load .env files
import sys # Import sys module for sys.exit

# --- (1) Dynamically find the project root path ---
current_dir = os.path.dirname(os.path.abspath(__file__))
# As config_loader.py is inside 2K_Program/Data/config,
# PROJECT_ROOT needs to go up two directories from the current directory.
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

# Assume .env file is in PROJECT_ROOT (2K_Program/)
DOTENV_PATH = os.path.join(PROJECT_ROOT, '.env')

# --- (2) Change config.yaml file path: Assume it's in the same directory as config_loader.py ---
# config.yaml is now inside the Data/config/ folder, so
# find it relative to current_dir (Data/config/).
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

# Load .env first when the script is loaded
load_environment_variables_initial()

# Create a logger object (logging setup is done via setup_logging in each Collector script's main())
logger = logging.getLogger(__name__)

# Declare global CONFIG variable. It will be populated after calling load_config.
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

        # Process database settings
        db_config = raw_config.get('database', {})
        processed_db_config = {}
        for db_key, db_value in db_config.items():
            if db_key.endswith('_env'): # Keys ending with _env reference environment variables
                env_var_name = str(db_value)
                env_var_value = os.getenv(env_var_name)
                if env_var_value:
                    processed_db_config[db_key[:-4]] = env_var_value # Remove _env and store with actual key name
                    logger.info(f"Loaded database '{db_key[:-4]}' information from environment variable '{env_var_name}'.")
                else:
                    logger.critical(f"Error: Environment variable '{env_var_name}' (database {db_key[:-4]}) not found. Ensure it's set in your '.env' file.")
                    raise ValueError(f"Required environment variable '{env_var_name}' is not set.")
            else:
                processed_db_config[db_key] = db_value
        processed_config['database'] = processed_db_config

        # Process API key settings
        api_keys_config = raw_config.get('api_keys', {})
        processed_api_keys = {}
        for key_name, value in api_keys_config.items():
            # Assume the value in config.yaml is the environment variable name
            env_var_name = str(value) # Ensure the value is a string
            api_key_value = os.getenv(env_var_name)
            if api_key_value:
                processed_api_keys[key_name] = api_key_value
                logger.info(f"Loaded API key '{key_name}' from environment variable '{env_var_name}'.")
            else:
                # API keys might not be mandatory, so handle as a warning. Can be changed to critical if needed.
                logger.warning(f"Warning: Environment variable '{env_var_name}' for API key '{key_name}' not found. Corresponding API calls might be skipped.")
                processed_api_keys[key_name] = None # Set to None if value is missing
        processed_config['api_keys'] = processed_api_keys

        # Directly copy other settings (unprocessed top-level keys)
        for key, value in raw_config.items():
            if key not in ['database', 'api_keys']: # Exclude already processed sections
                processed_config[key] = value
        
        return processed_config

    except yaml.YAMLError as exc:
        logger.critical(f"Error parsing config.yaml file: {exc}")
        raise # Re-raise exception on YAML parsing error
    except Exception as e:
        logger.critical(f"An unexpected error occurred while loading the configuration file: {e}", exc_info=True)
        raise # Re-raise any other unexpected errors

# Load the global CONFIG variable when the script is imported
try:
    CONFIG.update(load_config()) # Update the global CONFIG dictionary
except Exception as e:
    # This error might occur before the logging system is fully ready.
    # Therefore, print directly to sys.stderr and exit the program.
    print(f"CRITICAL ERROR: A critical error occurred while loading the configuration file. Program will terminate: {e}", file=sys.stderr)
    sys.exit(1)

# Code for testing (optional)
if __name__ == "__main__":
    # This block runs only when config_loader.py is executed directly.
    # Simple logging can be set up here for testing purposes.
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger.info("Testing config loading by running config_loader.py directly:")
    logger.info(f"Project Root: {PROJECT_ROOT}")
    logger.info(f".env Path: {DOTENV_PATH}")
    logger.info(f"config.yaml Path: {CONFIG_YAML_PATH}")
    
    if 'api_keys' in CONFIG:
        logger.info("\nAPI Keys (partially masked):")
        for key, value in CONFIG['api_keys'].items():
            if value and len(str(value)) > 4: # Check type of value and length
                logger.info(f"  {key}: {str(value)[:4]}...") # Show only first 4 characters and mask
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