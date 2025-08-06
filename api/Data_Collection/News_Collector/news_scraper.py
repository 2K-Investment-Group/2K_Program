import requests
import time
import random
import os
import json
from datetime import datetime, timedelta
import sys
import yaml

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from Data_Collection.config import config_loader
    from utils.logger_config import setup_logging
    import logging
except ImportError as e:
    print(f"FATAL ERROR: Could not import core configuration or logging modules. Check paths and class names. Error: {e}", file=sys.stderr)
    sys.exit(1)

setup_logging()
logger = logging.getLogger(__name__)
logger.info("News Scraper module initialized.")

def _get_api_date_params(api_name: str, days_ago: int = 30) -> dict:
    today = datetime.now()
    start_date = today - timedelta(days=days_ago) # Default 30 days ago

    params = {}
    if api_name == "AlphaVantage":
        params['time_from'] = start_date.strftime("%Y%m%dT%H%M")
        params['time_to'] = today.strftime("%Y%m%dT%H%M")
    elif api_name == "Finnhub":
        params['from'] = start_date.strftime("%Y-%m-%d")
        params['to'] = today.strftime("%Y-%m-%d")
    elif api_name == "NewsAPI":
        newsapi_start_date = today - timedelta(days=29)
        params['from'] = newsapi_start_date.isoformat(timespec='seconds') + 'Z'
        params['to'] = today.isoformat(timespec='seconds') + 'Z'
    elif api_name == "NewsData_io":
        params['from_date'] = start_date.strftime("%Y-%m-%d")
        params['to_date'] = today.strftime("%Y-%m-%d")
    return params

def fetch_news_from_api(api_config: dict, general_settings: dict, target_tickers_query: str = "") -> list:
    api_name = api_config.get('name', 'UnknownAPI')
    api_key_env_var = api_config.get('api_key_env_var')
    base_url = api_config.get('base_url')

    endpoint_params = api_config.get('endpoint_params', {}).copy() 
    api_key = os.getenv(api_key_env_var)
    if not api_key:
        logger.error(f"API key for {api_name} not found in environment variables (checked: {api_key_env_var}). Skipping this API.")
        return []

    delay = general_settings.get('delay_between_api_requests_seconds', 1)
    random_extra_delay = random.uniform(0, general_settings.get('random_delay_range_seconds', 0.5))
    time.sleep(delay + random_extra_delay)

    collected_articles_from_api = []

    date_params = _get_api_date_params(api_name)
    endpoint_params.update(date_params)

    if api_name == "AlphaVantage":
        endpoint_params['apikey'] = api_key
    elif api_name == "Finnhub":
        endpoint_params['token'] = api_key
    elif api_name == "NewsAPI":
        endpoint_params['apiKey'] = api_key
    elif api_name == "NewsData_io":
        endpoint_params['apikey'] = api_key

    if target_tickers_query:
        if api_name == 'AlphaVantage':
            if 'keywords' in endpoint_params:
                endpoint_params['keywords'] += f",{target_tickers_query.replace(' OR ', ',')}"
            elif 'topics' in endpoint_params: # AlphaVantage also uses 'topics'
                 endpoint_params['topics'] += f",{target_tickers_query.replace(' OR ', ',')}"
            else:
                endpoint_params['keywords'] = target_tickers_query.replace(' OR ', ',') # Default to keywords

        elif api_name == 'Finnhub':
            endpoint_params['category'] = 'company-news'
            endpoint_params['symbol'] = target_tickers_query.replace(' OR ', ',')
            logger.info(f"Finnhub API: Changed category to 'company-news' and added symbol(s): {endpoint_params['symbol']}")

        elif api_name == 'NewsAPI':
            current_q = endpoint_params.get('q', '')
            if current_q:
                endpoint_params['q'] = f"({current_q}) AND ({target_tickers_query})"
            else:
                endpoint_params['q'] = target_tickers_query

        elif api_name == 'NewsData_io':
            current_q = endpoint_params.get('q', '')
            if current_q:
                endpoint_params['q'] = f"({current_q}) OR ({target_tickers_query})"
            else:
                endpoint_params['q'] = target_tickers_query

    logger.info(f"Fetching news from {api_name} ({base_url}) with parameters: {endpoint_params}")

    try:
        response = requests.get(base_url, params=endpoint_params, headers=headers, timeout=30)
        response.raise_for_status() 
        data = response.json()

        if api_name == "AlphaVantage":
            if data and 'feed' in data:
                for article in data['feed']:
                    if article.get('title') and article.get('url'):
                        collected_articles_from_api.append({
                            'api_source': api_name,
                            'title': article.get('title'),
                            'summary': article.get('summary'),
                            'url': article.get('url'),
                            'published_at': article.get('time_published'),
                            'scraped_at': datetime.now().isoformat(),
                            'topics': article.get('topics', []),
                            'ticker_sentiment': article.get('ticker_sentiment', [])
                        })
            elif data.get("Note"):
                logger.warning(f"AlphaVantage API limit reached or other issue: {data.get('Note')}")
            else:
                logger.warning(f"AlphaVantage: No 'feed' data or unexpected response structure: {data}. Full response (first 500 chars): {json.dumps(data)[:500]}")
        elif api_name == "Finnhub":
            if data and isinstance(data, list):
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
                logger.warning(f"Finnhub: Unexpected response structure or no data: {data}. Full response (first 500 chars): {json.dumps(data)[:500]}")

        elif api_name == "NewsAPI":
            if data and data.get('status') == 'ok' and 'articles' in data:
                for article in data['articles']:
                    collected_articles_from_api.append({
                        'api_source': api_name,
                        'title': article.get('title'),
                        'summary': article.get('description'),
                        'url': article.get('url'),
                        'published_at': article.get('publishedAt'),
                        'scraped_at': datetime.now().isoformat(),
                        'source_name': article.get('source', {}).get('name'),
                        'author': article.get('author')
                    })
            elif data.get('status') == 'error':
                logger.warning(f"NewsAPI error: {data.get('code')} - {data.get('message')}. Full response (first 500 chars): {json.dumps(data)[:500]}")
            else:
                logger.warning(f"NewsAPI: Unexpected response structure or no articles: {data}. Full response (first 500 chars): {json.dumps(data)[:500]}")

        elif api_name == "NewsData_io":
            if data and data.get('status') == 'success' and 'results' in data:
                for article in data['results']:
                    collected_articles_from_api.append({
                        'api_source': api_name,
                        'title': article.get('title'),
                        'summary': article.get('description'),
                        'body': article.get('content'),
                        'url': article.get('link'),
                        'published_at': article.get('pubDate'),
                        'scraped_at': datetime.now().isoformat(),
                        'source_id': article.get('source_id'),
                        'creator': article.get('creator')
                    })
            elif data.get('status') == 'error':
                logger.warning(f"NewsData.io error: {data.get('code')} - {data.get('message')}. Full response (first 500 chars): {json.dumps(data)[:500]}")
            else:
                logger.warning(f"NewsData.io: Unexpected response structure or no results: {data}. Full response (first 500 chars): {json.dumps(data)[:500]}")
        else:
            logger.warning(f"Unknown API '{api_name}'. Please add specific handling for this API.")

    except requests.exceptions.RequestException as e:
        logger.error(f"{api_name} API request failed: {e}", exc_info=True)
    except json.JSONDecodeError as e:
        logger.error(f"{api_name} API response not JSON or empty: {e}. Response text: {response.text[:500]}...", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred while processing {api_name} response: {e}", exc_info=True)

    logger.info(f"Collected {len(collected_articles_from_api)} articles from {api_name}.")
    return collected_articles_from_api

def collect_news_articles_via_api(api_configs: list, general_settings: dict, news_api_settings: dict) -> list:
    """
    Collects news articles from multiple APIs based on configurations, applying ticker filters.
    :param api_configs: List of dictionaries, each defining an API source.
    :param general_settings: Dictionary of general API calling settings.
    :param news_api_settings: Dictionary containing news API settings, including target_tickers.
    :return: List of dictionaries, each representing a collected article.
    """
    all_collected_articles = []

    target_tickers = news_api_settings.get('target_tickers', [])
    target_tickers_query = " OR ".join(target_tickers) if target_tickers else ""

    if target_tickers:
        logger.info(f"News collection will target tickers: {', '.join(target_tickers)}")
    else:
        logger.warning("No 'target_tickers' specified in news_sources.yaml. Collecting general news based on API defaults.")

    for api_config in api_configs:
        if not api_config.get('enable', False):
            logger.info(f"API '{api_config['name']}' is disabled in news_sources.yaml. Skipping.")
            continue

        articles_from_this_api = fetch_news_from_api(api_config, general_settings, target_tickers_query)
        if articles_from_this_api:
            all_collected_articles.extend(articles_from_this_api)
            
    logger.info(f"Finished collecting a total of {len(all_collected_articles)} articles across all enabled APIs.")
    return all_collected_articles

if __name__ == "__main__":
    logger.info("--- Starting News Scraper direct execution ---")

    config_loader.load_env_variables()

    NEWS_SOURCES_CONFIG_PATH = os.path.join(project_root, 'Data_Collection', 'config', 'news_sources.yaml')
    NEWS_SOURCES_CONFIG = None
    try:
        with open(NEWS_SOURCES_CONFIG_PATH, 'r', encoding='utf-8') as f:
            NEWS_SOURCES_CONFIG = yaml.safe_load(f)
        logger.info(f"Successfully loaded news_sources.yaml from '{NEWS_SOURCES_CONFIG_PATH}' for direct execution.")
    except (FileNotFoundError, yaml.YAMLError) as e:
        logger.error(f"FATAL: news_sources.yaml could not be loaded. Error: {e}")
        sys.exit(1)

    news_api_settings = NEWS_SOURCES_CONFIG.get('news_api_config', {})
    
    if not news_api_settings.get('enable_api_collection', False):
        logger.info("API news collection is disabled in news_sources.yaml. Exiting.")
        sys.exit(0)

    general_api_settings = {
        'delay_between_api_requests_seconds': news_api_settings.get('delay_between_api_requests_seconds', 1),
        'random_delay_range_seconds': news_api_settings.get('random_delay_range_seconds', 0.5),
    }

    api_configs_to_use = news_api_settings.get('apis', [])
    
    if not api_configs_to_use:
        logger.warning("No news API configurations found in news_sources.yaml 'apis' section. No APIs to scrape.")
        sys.exit(0)

    collected_data = collect_news_articles_via_api(
        api_configs_to_use,
        general_api_settings,
        news_api_settings
    )

    output_dir = os.path.join(project_root, "Data", "storage")
    os.makedirs(output_dir, exist_ok=True)
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
        logger.info("No articles were collected during this direct run.")
    
    logger.info("--- News Scraper direct execution finished ---")