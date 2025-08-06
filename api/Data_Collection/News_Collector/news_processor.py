import pandas as pd
import json
import logging
import os

logger = logging.getLogger(__name__)

class NewsProcessor:
    def __init__(self, input_file_path: str):
        """
        Initializes the processor with the path to the raw news data file.
        
        Args:
            input_file_path (str): The path to the JSON file containing scraped news articles.
        """
        self.input_file_path = input_file_path
        if not os.path.exists(self.input_file_path):
            logger.error(f"Input file not found: {self.input_file_path}")
            raise FileNotFoundError(f"Input file not found: {self.input_file_path}")

    def _load_and_clean_data(self) -> pd.DataFrame:
        """
        Loads data from the JSON file, cleans it, and returns a DataFrame.
        - Loads JSON file.
        - Converts to DataFrame.
        - Drops duplicates based on 'url' and 'title'.
        - Converts 'published_at' to datetime objects.
        - Drops rows with invalid dates.
        """
        logger.info(f"Loading and cleaning data from {self.input_file_path}")
        try:
            with open(self.input_file_path, 'r', encoding='utf-8') as f:
                articles = json.load(f)
            logger.info(f"Successfully loaded {len(articles)} raw articles.")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {self.input_file_path}: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error loading data from {self.input_file_path}: {e}")
            return pd.DataFrame()

        if not articles:
            logger.warning("The loaded JSON file is empty. Returning an empty DataFrame.")
            return pd.DataFrame()

        df = pd.DataFrame(articles)

        initial_rows = len(df)
        if 'url' in df.columns:
            df.drop_duplicates(subset=['url'], inplace=True, keep='last', ignore_index=True)
        else:
            df.drop_duplicates(subset=['title'], inplace=True, keep='last', ignore_index=True)
        
        if len(df) < initial_rows:
            logger.info(f"Removed {initial_rows - len(df)} duplicate articles.")

        if 'published_at' in df.columns:
            df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce', utc=True)
            
            rows_before_dropna = len(df)
            df.dropna(subset=['published_at'], inplace=True)
            if len(df) < rows_before_dropna:
                logger.warning(f"Removed {rows_before_dropna - len(df)} rows due to invalid 'published_at' dates.")
        else:
            logger.warning("No 'published_at' column found. Date-based analysis will not be possible.")
        
        if 'sentiment_score' not in df.columns:
            logger.warning("Missing 'sentiment_score' column. Creating it with neutral value 0.0 for compatibility.")
            df['sentiment_score'] = 0.0

        logger.info(f"Processing complete. Retained {len(df)} unique and valid articles.")
        return df

    def process(self) -> pd.DataFrame:
        """
        The main public method to run the entire processing pipeline.
        
        Returns:
            pd.DataFrame: A cleaned and preprocessed DataFrame ready for analysis.
        """
        return self._load_and_clean_data()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    try:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))
        test_input_path = os.path.join(project_root, 'Data', 'storage', 'scraped_api_news_articles.json')
        
        if os.path.exists(test_input_path):
            logger.info(f"--- Running NewsProcessor in standalone test mode ---")
            processor = NewsProcessor(input_file_path=test_input_path)
            processed_dataframe = processor.process()
            
            if not processed_dataframe.empty:
                print("\n--- Processor Test Output ---")
                print(f"DataFrame shape: {processed_dataframe.shape}")
                print("\nDataFrame Info:")
                processed_dataframe.info()
                print("\nFirst 5 rows of the processed DataFrame:")
                print(processed_dataframe.head())
                print("\n--- Standalone test finished successfully ---")
            else:
                print("\n--- Processor Test finished: No data was processed. ---")
        else:
            logger.warning(f"Test file not found, cannot run standalone test: {test_input_path}")
    except Exception as e:
        logger.error(f"An error occurred during standalone test: {e}", exc_info=True)