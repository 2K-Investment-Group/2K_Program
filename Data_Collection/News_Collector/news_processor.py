import json
import os
from collections import Counter, defaultdict
from datetime import datetime, date # date 타입 임포트
import logging
import pandas as pd # pandas는 현재 사용되지 않으나, 추후 데이터프레임 변환에 유용할 수 있습니다.

# --- Logger setup (Moved to __main__ for better control) ---
# This part will be handled in the main execution block to avoid re-initializing
# the logger with every instance of NewsProcessor.

class NewsProcessor:
    def __init__(self, input_file_path: str):
        self.input_file_path = input_file_path
        self.articles = []
        self.logger = logging.getLogger(__name__) # Class-specific logger instance
        self.load_data()

    def load_data(self):
        """저장된 JSON 파일에서 뉴스 기사를 로드합니다."""
        if not os.path.exists(self.input_file_path):
            self.logger.error(f"Error: Input file not found at {self.input_file_path}")
            return

        try:
            with open(self.input_file_path, 'r', encoding='utf-8') as f:
                self.articles = json.load(f)
            self.logger.info(f"Successfully loaded {len(self.articles)} articles from {self.input_file_path}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON from {self.input_file_path}: {e}")
            self.articles = []
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while loading data: {e}")
            self.articles = []

    def preprocess_articles(self):
        """
        기사를 전처리합니다. (예: 중복 제거, 날짜 형식 통일)
        'url'을 기준으로 중복을 제거합니다.
        """
        if not self.articles:
            self.logger.warning("No articles to preprocess.")
            return []

        unique_articles = {}
        no_url_articles = [] # URL이 없는 기사들을 별도로 저장
        
        for article in self.articles:
            url = article.get('url')
            if url:
                # 동일 URL이 여러 번 수집될 경우, 가장 최근에 스크래핑된 기사를 유지합니다.
                # scraped_at 필드가 없을 경우를 대비하여 기본값 설정
                current_scraped_at = article.get('scraped_at', '0000-00-00T00:00:00')
                existing_scraped_at = unique_articles.get(url, {}).get('scraped_at', '0000-00-00T00:00:00')
                
                if url not in unique_articles or current_scraped_at > existing_scraped_at:
                    unique_articles[url] = article
            else:
                # URL이 없는 기사는 일단 별도의 리스트에 추가 (중복 처리는 나중에 필요 시)
                self.logger.warning(f"Article with no URL found, skipping URL-based deduplication: {article.get('title', 'No Title')}")
                no_url_articles.append(article)
                
        processed_list = list(unique_articles.values())
        
        # URL 없는 기사들에 대한 별도 처리 (예: 제목+발행일 기준 재중복 제거 후 추가)
        # 현재는 단순히 추가만 함. 더 정교한 중복 제거 필요 시 로직 추가
        if no_url_articles:
            # 여기서는 URL이 없는 기사들 내부에서 중복을 제거하지 않고 단순히 추가합니다.
            # 필요하다면, 'title', 'published_at', 'summary' 등을 조합하여 중복을 제거할 수 있습니다.
            self.logger.info(f"Adding {len(no_url_articles)} articles with no URL (no further deduplication for these).")
            processed_list.extend(no_url_articles)

        self.logger.info(f"Preprocessed: {len(self.articles)} articles loaded, {len(processed_list)} unique/processed articles retained.")
        return processed_list

    def analyze_basic_stats(self, articles: list):
        """기본 통계 (총 기사 수, 유니크 기사 수)를 분석합니다."""
        total_loaded = len(self.articles)
        unique_count = len(articles)
        self.logger.info("\n--- Basic Statistics ---")
        self.logger.info(f"Total articles loaded: {total_loaded}")
        self.logger.info(f"Unique articles after preprocessing: {unique_count}")
        return {"total_loaded": total_loaded, "unique_count": unique_count}

    def analyze_articles_by_date(self, articles: list):
        """날짜별 기사 분포를 분석합니다."""
        date_counts = Counter()
        for article in articles:
            published_at = article.get('published_at')
            if published_at:
                try:
                    # '20250711T172220' (AlphaVantage)
                    if 'T' in published_at and len(published_at) == 15 and published_at[8] == 'T': 
                        parsed_date = datetime.strptime(published_at, "%Y%m%dT%H%M%S").date()
                    # '2025-07-11T17:22:20.631224Z' or '2025-07-11T17:22:20' (ISO 8601 with/without Z/microseconds)
                    elif 'T' in published_at and '-' in published_at:
                        # fromisoformat은 Z를 +00:00으로 자동 변환, 마이크로초 처리 가능
                        parsed_date = datetime.fromisoformat(published_at.replace('Z', '+00:00')).date()
                    # '2025-07-11' (YYYY-MM-DD)
                    elif published_at.count('-') == 2: 
                        parsed_date = datetime.strptime(published_at, "%Y-%m-%d").date()
                    else: # 기타 알 수 없는 형식
                        self.logger.warning(f"Could not parse published_at date (unknown format): {published_at}")
                        continue # 다음 기사로 건너뛰기
                        
                    date_counts[parsed_date.isoformat()] += 1
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"Error parsing published_at date '{published_at}': {e}")
            else:
                self.logger.warning(f"Article missing 'published_at' field: {article.get('title', 'No Title')}")
        
        sorted_dates = sorted(date_counts.items())
        self.logger.info("\n--- Articles by Publish Date ---")
        for date_str, count in sorted_dates:
            self.logger.info(f"{date_str}: {count} articles")
        return dict(sorted_dates)

    def analyze_articles_by_source(self, articles: list):
        """API 소스별 기사 분포를 분석합니다."""
        source_counts = Counter()
        for article in articles:
            api_source = article.get('api_source')
            if api_source:
                source_counts[api_source] += 1
            else:
                self.logger.warning(f"Article missing 'api_source' field: {article.get('title', 'No Title')}")
        
        self.logger.info("\n--- Articles by API Source ---")
        for source, count in source_counts.most_common():
            self.logger.info(f"{source}: {count} articles")
        return dict(source_counts.most_common())

    def analyze_topics(self, articles: list, top_n: int = 10):
        """가장 많이 언급된 주제를 분석합니다."""
        topic_scores = defaultdict(float)
        topic_counts = Counter()

        for article in articles:
            # AlphaVantage의 'topics' 필드에 대한 처리 (list of dicts)
            topics = article.get('topics', []) 
            
            # NewsAPI나 Finnhub 등 다른 API는 'topics' 필드가 없을 수 있음
            if not topics and article.get('api_source') not in ["AlphaVantage"]:
                # 다른 API의 경우 주제 추출을 위해 'summary'나 'title'에서 키워드 분석이 필요할 수 있음
                # 여기서는 'topics' 필드가 있는 경우만 처리한다고 가정합니다.
                continue 

            for topic_info in topics:
                topic_name = topic_info.get('topic')
                relevance_score = float(topic_info.get('relevance_score', 0.0))
                if topic_name:
                    topic_scores[topic_name] += relevance_score
                    topic_counts[topic_name] += 1
        
        if not topic_counts:
            self.logger.info("No topic data available for analysis.")
            return {}

        avg_topic_scores = {
            topic: topic_scores[topic] / topic_counts[topic]
            for topic in topic_counts
        }

        # 관련성 점수의 합계 기준으로 정렬 (더 자주 언급되고 관련성이 높은 주제)
        sorted_topics = sorted(topic_scores.items(), key=lambda item: item[1], reverse=True)
        
        self.logger.info(f"\n--- Top {top_n} Topics by Total Relevance Score ---")
        results = {}
        for topic, total_score in sorted_topics[:top_n]:
            avg_score = avg_topic_scores[topic]
            count = topic_counts[topic]
            self.logger.info(f"Topic: {topic} | Total Score: {total_score:.2f} | Avg. Score: {avg_score:.2f} | Count: {count}")
            results[topic] = {"total_relevance_score": total_score, "average_relevance_score": avg_score, "count": count}
        return results

    def analyze_ticker_sentiment(self, articles: list, top_n: int = 10):
        """
        티커별 감성 분석을 수행합니다.
        가장 많이 언급된 티커와 해당 티커의 평균 감성 점수를 계산합니다.
        """
        ticker_sentiment_sum = defaultdict(float)
        ticker_sentiment_count = defaultdict(int)
        ticker_sentiment_labels = defaultdict(Counter) # 각 티커별 감성 레이블 분포

        for article in articles:
            # AlphaVantage의 'ticker_sentiment' 필드에 대한 처리 (list of dicts)
            ticker_sentiments = article.get('ticker_sentiment', [])
            
            # 다른 API는 'ticker_sentiment' 필드가 없을 수 있음
            if not ticker_sentiments and article.get('api_source') not in ["AlphaVantage"]:
                continue 

            for ts in ticker_sentiments:
                ticker = ts.get('ticker')
                try:
                    sentiment_score = float(ts.get('ticker_sentiment_score', 0.0))
                except (ValueError, TypeError):
                    self.logger.warning(f"Invalid ticker_sentiment_score for ticker '{ticker}': {ts.get('ticker_sentiment_score')}")
                    sentiment_score = 0.0 # 기본값 또는 스킵 처리
                
                sentiment_label = ts.get('ticker_sentiment_label', 'Unknown')
                # relevance_score = float(ts.get('relevance_score', 0.0)) # 관련성 점수 고려 가능 (옵션)

                if ticker:
                    ticker_sentiment_sum[ticker] += sentiment_score
                    ticker_sentiment_count[ticker] += 1
                    ticker_sentiment_labels[ticker][sentiment_label] += 1
        
        if not ticker_sentiment_count:
            self.logger.info("No ticker sentiment data available for analysis.")
            return {}

        # 티커별 평균 감성 점수 계산
        avg_ticker_sentiments = {
            ticker: ticker_sentiment_sum[ticker] / ticker_sentiment_count[ticker]
            for ticker, count in ticker_sentiment_count.items() if count > 0
        }

        # 가장 많이 언급된 티커를 기준으로 정렬
        sorted_tickers_by_count = sorted(ticker_sentiment_count.items(), key=lambda item: item[1], reverse=True)

        self.logger.info(f"\n--- Top {top_n} Tickers by Mention Count and Sentiment ---")
        results = {}
        for ticker, count in sorted_tickers_by_count[:top_n]:
            avg_score = avg_ticker_sentiments.get(ticker, 0.0)
            # 감성 점수에 따라 레이블 결정 (편의상)
            if avg_score > 0.35:
                overall_label = "Bullish"
            elif avg_score > 0.05:
                overall_label = "Somewhat-Bullish"
            elif avg_score < -0.35:
                overall_label = "Bearish"
            elif avg_score < -0.05:
                overall_label = "Somewhat-Bearish"
            else:
                overall_label = "Neutral"

            self.logger.info(f"Ticker: {ticker} | Mentions: {count} | Avg. Sentiment Score: {avg_score:.4f} ({overall_label})")
            self.logger.info(f"  Sentiment Distribution: {dict(ticker_sentiment_labels[ticker].most_common())}")
            results[ticker] = {
                "mentions": count,
                "average_sentiment_score": avg_score,
                "overall_sentiment_label": overall_label,
                "sentiment_distribution": dict(ticker_sentiment_labels[ticker].most_common())
            }
        return results

    def run_analysis(self, top_n_topics: int = 10, top_n_tickers: int = 10):
        """전체 분석 프로세스를 실행하고 결과를 반환합니다."""
        self.logger.info("Starting news data analysis...")
        
        processed_articles = self.preprocess_articles()
        
        analysis_results = {
            "basic_stats": self.analyze_basic_stats(processed_articles),
            "articles_by_date": self.analyze_articles_by_date(processed_articles),
            "articles_by_source": self.analyze_articles_by_source(processed_articles),
            "top_topics": self.analyze_topics(processed_articles, top_n=top_n_topics),
            "ticker_sentiment": self.analyze_ticker_sentiment(processed_articles, top_n=top_n_tickers)
        }
        
        self.logger.info("News data analysis finished.")
        return analysis_results

if __name__ == "__main__":
    # --- Logger setup for the main script ---
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Configure root logger or a specific logger instance
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(log_dir, f"news_processor_{timestamp}.log")),
            logging.StreamHandler(),
        ],
    )
    # Get logger for this module
    main_logger = logging.getLogger(__name__)
    main_logger.info("News Processor script started.")

    # 데이터가 저장된 파일 경로
    # scraped_api_news_articles.json 파일은 Data/storage 디렉토리에 있다고 가정
    # os.path.dirname(os.path.abspath(__file__))는 현재 파일 (news_processor.py)의 디렉토리
    # os.path.dirname(...)으로 한 번 더 감싸면 상위 디렉토리 (Data_Collection)
    # 또 한 번 감싸면 프로젝트 루트 디렉토리 (e.g., your_project_root)
    # 현재 `news_processor.py`가 `your_project_root/Data_Collection/News_Processing/news_processor.py`에 있다면
    # `os.path.dirname(os.path.dirname(os.path.abspath(__file__)))`가 `your_project_root`가 됩니다.
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
    data_file_path = os.path.join(base_dir, "Data", "storage", "scraped_api_news_articles.json")

    processor = NewsProcessor(data_file_path)
    
    # 분석 실행 및 결과 저장
    analysis_results = processor.run_analysis(top_n_topics=10, top_n_tickers=10)

    # 필요하다면 분석 결과를 JSON 파일로 저장
    output_analysis_dir = os.path.join(base_dir, "Data", "analysis_results")
    os.makedirs(output_analysis_dir, exist_ok=True)
    output_analysis_filename = f"news_analysis_results_{timestamp}.json"
    output_analysis_path = os.path.join(output_analysis_dir, output_analysis_filename)

    try:
        with open(output_analysis_path, 'w', encoding='utf-8') as f:
            json.dump(analysis_results, f, ensure_ascii=False, indent=4)
        main_logger.info(f"Analysis results saved to {output_analysis_path}")
    except Exception as e:
        main_logger.error(f"Failed to save analysis results to {output_analysis_path}: {e}", exc_info=True)