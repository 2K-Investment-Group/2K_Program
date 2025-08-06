import json
import os
import sys
from datetime import datetime
import logging
import yaml
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    import Data_Collection.config.config_loader as config_loader_module
    from utils.logger_config import setup_logging
except ImportError as e:
    print(f"FATAL ERROR: Could not import core configuration or logging modules. Check paths and class names. Error: {e}", file=sys.stderr)
    sys.exit(1)

setup_logging()
logger = logging.getLogger(__name__)
logger.info("Main collector script started.")

try:
    from Data_Collection.News_Collector.news_scraper import collect_news_articles_via_api
    from Data_Collection.News_Collector.news_processor import NewsProcessor
    from Data_Collection.News_Collector.news_analyzer import NewsAnalyzer
    logger.info("Successfully imported News Scraper, Processor, and Analyzer modules.")
except ImportError as e:
    logger.error(f"Failed to import a required module. Error: {e}", exc_info=True)
    sys.exit(1)

def convert_dict_keys_to_str(data):
    """Recursively converts dict keys (especially date objects) to strings for JSON serialization."""
    if isinstance(data, dict):
        return {str(k): convert_dict_keys_to_str(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_dict_keys_to_str(elem) for elem in data]
    else:
        return data

def run_news_collection_process(storage_output_dir: str) -> str | None:
    """Runs the news scraping process and saves the raw data."""
    logger.info("--- Starting News Collection ---")
    
    try:
        news_sources_config_path = os.path.join(project_root, 'Data_Collection', 'config', 'news_sources.yaml')
        with open(news_sources_config_path, 'r', encoding='utf-8') as f:
            news_sources_config = yaml.safe_load(f)
        logger.info(f"Successfully loaded news_sources.yaml from {news_sources_config_path}")
    except (FileNotFoundError, yaml.YAMLError) as e:
        logger.error(f"Error with news_sources.yaml: {e}. Cannot proceed with news collection.", exc_info=True)
        return None

    news_api_settings = news_sources_config.get('news_api_config', {})
    if not news_api_settings.get('enable_api_collection', False):
        logger.info("API news collection is disabled in news_sources.yaml. Skipping collection.")
        return None

    general_api_settings = {
        'delay_between_api_requests_seconds': news_api_settings.get('delay_between_api_requests_seconds', 1),
        'random_delay_range_seconds': news_api_settings.get('random_delay_range_seconds', 0.5),
    }
    api_configs_to_use = news_api_settings.get('apis', [])
    
    if not api_configs_to_use:
        logger.warning("No API configurations found in news_sources.yaml. No APIs to scrape.")
        return None

    collected_data = collect_news_articles_via_api(
        api_configs_to_use,
        general_api_settings,
        news_api_settings 
    )

    output_filename = news_api_settings.get('output_filename', "scraped_api_news_articles.json")
    output_path = os.path.join(storage_output_dir, output_filename)
    
    if collected_data:
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(collected_data, f, ensure_ascii=False, indent=4)
            logger.info(f"Successfully saved {len(collected_data)} articles to {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Failed to save collected data to {output_path}: {e}", exc_info=True)
            return None
    else:
        logger.info("No articles were collected during this run.")
        return None

if __name__ == "__main__":
    logger.info("--- 🚀 Starting Main News Collection and Analysis Process ---")

    storage_dir = os.path.join(project_root, "Data", "storage")
    analysis_dir = os.path.join(project_root, "Data", "analysis_results")
    os.makedirs(storage_dir, exist_ok=True)
    os.makedirs(analysis_dir, exist_ok=True)
    
    scraped_articles_path = os.path.join(storage_dir, "scraped_api_news_articles.json")
    
    collected_file_path = run_news_collection_process(storage_dir)

    input_file_for_processing = collected_file_path if collected_file_path else scraped_articles_path
    
    if not os.path.exists(input_file_for_processing):
        logger.error(f"No news data file found at {input_file_for_processing}. Cannot proceed.")
        sys.exit(1)
    
    logger.info(f"Using data file for processing and analysis: {input_file_for_processing}")

    logger.info("--- 🛠️ Starting News Processing ---")
    try:
        processor = NewsProcessor(input_file_path=input_file_for_processing)
        processed_df = processor.process()
        
        if processed_df.empty:
            logger.error("Processing resulted in an empty DataFrame. Cannot proceed to analysis.")
            sys.exit(1)
            
        logger.info("News processing finished successfully. DataFrame is ready for analysis.")
    except Exception as e:
        logger.error(f"A critical error occurred during news processing: {e}", exc_info=True)
        sys.exit(1)

    my_specific_analysis_tickers = [
    # 기존
    "AAPL", "MSFT", "GOOG", "SMCI", "NVDA", "TSLA", "AMZN", "AMD", "CRYPTO:BTC", "SPY",
    
    # 미국 Big Tech
    "META", "NFLX", "INTC", "IBM", "ORCL", "CSCO", "CRM", "ADBE", "AVGO", "QCOM",
    
    # AI / 반도체
    "ASML", "AMAT", "MU", "TXN", "LRCX", "TSM", "ON", "MRVL", "PLTR", "PATH",
    
    # EV 및 클린에너지
    "RIVN", "LCID", "NIO", "XPEV", "BYDDF", "F", "GM", "CHPT", "BLNK", "ENPH",
    
    # 에너지 / 원자재
    "XOM", "CVX", "COP", "SLB", "BP", "OXY", "SHEL", "VLO", "MPC", "PSX",
    
    # 헬스케어
    "JNJ", "PFE", "MRK", "ABBV", "LLY", "UNH", "TMO", "ABT", "BMY", "VRTX",
    
    # 금융/핀테크
    "JPM", "BAC", "WFC", "GS", "MS", "SCHW", "PYPL", "SQ", "MA", "V",
    
    # ETF 섹터/테마
    "QQQ", "VTI", "ARKK", "IWM", "DIA", "VOO", "SMH", "XLK", "XLF", "XLE",
    
    # Crypto 확장
    "CRYPTO:ETH", "CRYPTO:SOL", "CRYPTO:ADA", "CRYPTO:MATIC", "CRYPTO:AVAX", "CRYPTO:XRP", "CRYPTO:DOGE", "CRYPTO:DOT", "CRYPTO:LINK", "CRYPTO:TON"
    ]

    logger.info("--- 📊 Starting News Analysis ---")
    try:
        analyzer = NewsAnalyzer(dataframe=processed_df)
        
        analysis_results = analyzer.run_analysis(
            top_n_topics=50,
            top_n_tickers=5, 
            sentiment_interval='D',
            specific_tickers=my_specific_analysis_tickers
        )
        
        if not analysis_results:
            logger.error("News analysis failed or returned no results.")
            sys.exit(1)

    except Exception as e:
        logger.error(f"A critical error occurred during news analysis: {e}", exc_info=True)
        sys.exit(1)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_analysis_filename = f"news_analysis_summary_{timestamp}.json"
    output_analysis_path = os.path.join(analysis_dir, output_analysis_filename)

    try:
        json_serializable_results = convert_dict_keys_to_str(analysis_results)
        
        with open(output_analysis_path, 'w', encoding='utf-8') as f:
            json.dump(json_serializable_results, f, ensure_ascii=False, indent=4)
        logger.info(f"✅ Analysis summary successfully saved to {output_analysis_path}")
    except Exception as e:
        logger.error(f"Failed to save analysis summary: {e}", exc_info=True)
    
    logger.info("--- 🎉 Main News Collection and Analysis Process Finished ---")