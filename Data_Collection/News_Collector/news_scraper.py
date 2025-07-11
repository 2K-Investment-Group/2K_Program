import requests
import time
import random
import os
import json
from datetime import datetime, timedelta # timedelta 추가
import sys
import yaml # To load news_sources.yaml
# from urllib.parse import urljoin # Less critical with direct API base_urls, can remove if not used elsewhere

# Project root path setup
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

# Add project_root to sys.path to allow imports from Data.config and utils.logger_config
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import necessary modules from project structure
try:
    # Ensure these paths are correct relative to project_root
    # Assuming config_loader is in Data/config/config_loader.py
    # Assuming setup_logging is in utils/logger_config.py
    from Data_Collection.config import config_loader 
    from utils.logger_config import setup_logging
    import logging
except ImportError as e:
    print(f"Error importing core modules: {e}")
    print("Please ensure 'Data_Collection/config/config_loader.py' and 'utils/logger_config.py' exist and are accessible from the project root.")
    sys.exit(1)

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

def _get_api_date_params(api_name: str, days_ago: int = 30) -> dict:
    """
    각 API의 날짜 파라미터 형식을 고려하여 동적인 날짜 범위를 생성합니다.
    무료 API는 보통 과거 데이터 기간이 제한적이므로, 기본값은 최근 30일 이내로 설정.
    """
    today = datetime.now()
    start_date = today - timedelta(days=days_ago) # 기본 30일 전

    params = {}
    if api_name == "AlphaVantage":
        # AlphaVantage는 time_from, time_to 형식이 YYYYMMDDTHHMM
        params['time_from'] = start_date.strftime("%Y%m%dT%H%M")
        params['time_to'] = today.strftime("%Y%m%dT%H%M")
    elif api_name == "Finnhub":
        # Finnhub는 from, to 형식이 YYYY-MM-DD
        params['from'] = start_date.strftime("%Y-%m-%d")
        params['to'] = today.strftime("%Y-%m-%d")
    elif api_name == "NewsAPI":
        # NewsAPI는 from, to 형식이 ISO 8601 (YYYY-MM-DDTHH:MM:SSZ)
        # 무료 플랜은 'from' 파라미터가 30일 이내로 제한되므로, 최대 29일 전으로 설정.
        # 즉, 오늘 (포함) + 29일 전 = 총 30일 기간
        newsapi_start_date = today - timedelta(days=29) 
        params['from'] = newsapi_start_date.isoformat(timespec='seconds') + 'Z'
        params['to'] = today.isoformat(timespec='seconds') + 'Z'
    elif api_name == "NewsData_io":
        # NewsData.io는 from_date, to_date 형식이 YYYY-MM-DD
        params['from_date'] = start_date.strftime("%Y-%m-%d")
        params['to_date'] = today.strftime("%Y-%m-%d")
    return params

def fetch_news_from_api(api_config, general_settings):
    """
    Fetches news articles from a specific API based on its configuration.
    :param api_config: Dictionary containing configuration for a single API (from news_sources.yaml).
    :param general_settings: Dictionary of general API calling settings (delay).
    :return: List of dictionaries, each representing a collected article from this API, or None.
    """
    api_name = api_config.get('name', 'UnknownAPI')
    api_key_env_var = api_config.get('api_key_env_var')
    base_url = api_config.get('base_url')
    
    # endpoint_params를 복사하여 변경 가능하도록 함
    endpoint_params = api_config.get('endpoint_params', {}).copy() 
    
    api_key = os.getenv(api_key_env_var)
    if not api_key:
        logger.error(f"API key for {api_name} not found in environment variables (checked: {api_key_env_var}). Skipping this API.")
        return None

    # Apply general delay
    delay = general_settings.get('delay_between_api_requests_seconds', 1)
    random_extra_delay = random.uniform(0, general_settings.get('random_delay_range_seconds', 0.5))
    time.sleep(delay + random_extra_delay)

    headers = {}
    collected_articles_from_api = []

    # API별 날짜 파라미터 동적 추가 (기존 endpoint_params에 덮어쓰기)
    date_params = _get_api_date_params(api_name)
    endpoint_params.update(date_params)

    # API 키를 파라미터에 추가 (API마다 키 파라미터 이름이 다를 수 있음)
    if api_name == "AlphaVantage":
        endpoint_params['apikey'] = api_key
    elif api_name == "Finnhub":
        endpoint_params['token'] = api_key
    elif api_name == "NewsAPI":
        endpoint_params['apiKey'] = api_key
    elif api_name == "NewsData_io":
        endpoint_params['apikey'] = api_key
    
    logger.info(f"Fetching news from {api_name} ({base_url}) with parameters: {endpoint_params}")

    try:
        response = requests.get(base_url, params=endpoint_params, headers=headers, timeout=30)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        
        if api_name == "AlphaVantage":
            if data and 'feed' in data:
                for article in data['feed']:
                    title = article.get('title')
                    summary = article.get('summary')
                    url = article.get('url')
                    time_published = article.get('time_published') 
                    
                    if title and url:
                        collected_articles_from_api.append({
                            'api_source': api_name,
                            'title': title,
                            'summary': summary,
                            'url': url,
                            'published_at': time_published,
                            'scraped_at': datetime.now().isoformat(),
                            'topics': article.get('topics', []),
                            'ticker_sentiment': article.get('ticker_sentiment', []) # 금융 뉴스 특화 데이터
                        })
            elif data.get("Note"): # AlphaVantage 무료 플랜 제한 메시지
                logger.warning(f"AlphaVantage API limit reached or other issue: {data.get('Note')}")
            else:
                logger.warning(f"AlphaVantage: No 'feed' data or unexpected response structure: {data}")

        elif api_name == "Finnhub":
            if data and isinstance(data, list): # Finnhub는 리스트 형태의 JSON 반환
                for article in data:
                    published_at_iso = None
                    if article.get('datetime'):
                        try:
                            published_at_iso = datetime.fromtimestamp(article['datetime']).isoformat()
                        except (TypeError, ValueError) as e:
                            logger.warning(f"Finnhub: Could not parse datetime '{article.get('datetime')}': {e}")

                    collected_articles_from_api.append({
                        'api_source': api_name,
                        'title': article.get('headline'),
                        'summary': article.get('summary'),
                        'url': article.get('url'),
                        'published_at': published_at_iso,
                        'scraped_at': datetime.now().isoformat(),
                        'image_url': article.get('image'),
                        'related_symbols': article.get('related')
                    })
            else:
                logger.warning(f"Finnhub: Unexpected response structure or no data: {data}")

        elif api_name == "NewsAPI":
            if data and data.get('status') == 'ok' and 'articles' in data:
                for article in data['articles']:
                    collected_articles_from_api.append({
                        'api_source': api_name,
                        'title': article.get('title'),
                        'summary': article.get('description'), # NewsAPI에서는 description이 요약
                        'url': article.get('url'),
                        'published_at': article.get('publishedAt'),
                        'scraped_at': datetime.now().isoformat(),
                        'source_name': article.get('source', {}).get('name'),
                        'author': article.get('author')
                    })
            elif data.get('status') == 'error':
                logger.warning(f"NewsAPI error: {data.get('code')} - {data.get('message')}")
            else:
                logger.warning(f"NewsAPI: Unexpected response structure or no articles: {data}")
            
        elif api_name == "NewsData_io":
            if data and data.get('status') == 'success' and 'results' in data:
                for article in data['results']:
                    collected_articles_from_api.append({
                        'api_source': api_name,
                        'title': article.get('title'),
                        'summary': article.get('description'),
                        'body': article.get('content'), # NewsData.io는 본문 제공 가능성 있음
                        'url': article.get('link'), # 'url' 대신 'link' 사용
                        'published_at': article.get('pubDate'),
                        'scraped_at': datetime.now().isoformat(),
                        'source_id': article.get('source_id'),
                        'creator': article.get('creator')
                    })
            elif data.get('status') == 'error':
                logger.warning(f"NewsData.io error: {data.get('code')} - {data.get('message')}")
            else:
                logger.warning(f"NewsData.io: Unexpected response structure or no results: {data}")

        else:
            logger.warning(f"Unknown API '{api_name}'. Please add specific handling for this API.")

    except requests.exceptions.RequestException as e:
        logger.error(f"{api_name} API request failed: {e}", exc_info=True)
    except json.JSONDecodeError as e:
        logger.error(f"{api_name} API response not JSON or empty: {e}. Response text: {response.text[:500]}...", exc_info=True) # Partial response for debugging
    except Exception as e:
        logger.error(f"An unexpected error occurred while processing {api_name} response: {e}", exc_info=True)

    logger.info(f"Collected {len(collected_articles_from_api)} articles from {api_name}.")
    return collected_articles_from_api

def collect_news_articles_via_api(api_configs, general_settings):
    """
    Collects news articles from multiple APIs based on configurations.
    :param api_configs: List of dictionaries, each defining an API source.
    :param general_settings: Dictionary of general API calling settings.
    :return: List of dictionaries, each representing a collected article.
    """
    all_collected_articles = []
    
    for api_config in api_configs:
        articles_from_this_api = fetch_news_from_api(api_config, general_settings)
        if articles_from_this_api:
            all_collected_articles.extend(articles_from_this_api)
            
    logger.info(f"Finished collecting total {len(all_collected_articles)} articles across all APIs.")
    return all_collected_articles


if __name__ == "__main__":
    # 1. Load global configuration (Data/config.yaml) and .env
    # Assuming config_loader.py handles loading .env and populating os.environ
    # and possibly a global CONFIG dictionary if needed.
    # For this script, we primarily rely on os.getenv for API keys.
    config_loader.load_env_variables() # Ensure .env variables are loaded

    # 2. Load news specific configuration (Data_Collection/config/news_sources.yaml)
    news_sources_config_path = os.path.abspath(os.path.join(current_dir, os.pardir, 'config', 'news_sources.yaml'))
    
    try:
        with open(news_sources_config_path, 'r', encoding='utf-8') as f:
            news_sources_config = yaml.safe_load(f)
        logger.info(f"Loaded news sources config from {news_sources_config_path}")
    except FileNotFoundError:
        logger.error(f"Error: news_sources.yaml not found at {news_sources_config_path}. Please create it.")
        sys.exit(1)
    except yaml.YAMLError as e:
        logger.error(f"Error parsing news_sources.yaml: {e}")
        sys.exit(1)

    # Use 'news_api_config' section now
    news_api_settings = news_sources_config.get('news_api_config', {})
    
    if not news_api_settings.get('enable_api_collection', False):
        logger.info("API news collection is disabled in news_sources.yaml. Exiting.")
        sys.exit(0)

    # Extract general API calling settings from news_sources.yaml
    general_api_settings = {
        'delay_between_api_requests_seconds': news_api_settings.get('delay_between_api_requests_seconds', 1),
        'random_delay_range_seconds': news_api_settings.get('random_delay_range_seconds', 0.5),
    }

    # Extract individual API configurations
    api_configs_to_use = news_api_settings.get('apis', [])
    
    if not api_configs_to_use:
        logger.warning("No news API configurations found in news_sources.yaml. Please add API details to scrape.")
        sys.exit(0)
    
    # Start news collection via APIs
    collected_data = collect_news_articles_via_api(
        api_configs_to_use,
        general_api_settings
    )

    # Save collected data to Data/storage folder
    output_dir = os.path.join(project_root, "Data", "storage")
    os.makedirs(output_dir, exist_ok=True) # Create directory if it doesn't exist
    output_filename = news_api_settings.get('output_filename', "scraped_api_news_articles.json")
    output_path = os.path.join(output_dir, output_filename)
    
    if collected_data:
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(collected_data, f, ensure_ascii=False, indent=4)
            logger.info(f"Successfully saved {len(collected_data)} articles to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save collected data to {output_path}: {e}", exc_info=True)
    else:
        logger.info("No articles were collected.")