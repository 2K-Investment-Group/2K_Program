# news_analyzer.py (Version 3.0: Ultra-Sensitive Quantitative Analysis)

import pandas as pd
import logging
import os
import json
import requests
import time # API 호출 간의 지연을 위해 추가
from collections import defaultdict
import google.generativeai as genai

logger = logging.getLogger(__name__)

class NewsAnalyzer:
    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe
        logger.info(f"NewsAnalyzer initialized with a DataFrame of shape {self.df.shape}.")
        self.llm_model = None
        try:
            gemini_api_key = os.getenv("GEMINI_API_KEY")
            if not gemini_api_key:
                logger.warning("GEMINI_API_KEY not found. LLM analysis will be skipped.")
            else:
                genai.configure(api_key=gemini_api_key)
                self.llm_model = genai.GenerativeModel('gemini-1.5-pro-latest') # 고성능 모델 사용
                logger.info("Gemini 1.5 Pro model initialized for high-sensitivity analysis.")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini LLM model: {e}", exc_info=True)

    def _search_google_for_news(self, ticker: str, total_articles=10) -> list:
        """
        CHANGED: Fetches up to 100 articles by paginating through Google Search API results.
        """
        api_key = os.getenv("GOOGLE_API_KEY")
        pse_id = os.getenv("PSE_ID")
        if not api_key or not pse_id:
            logger.warning("GOOGLE_API_KEY or PSE_ID not found. Google search is skipped.")
            return []

        all_articles = []
        num_requests = (total_articles + 9) // 10  # 100개 기사를 위해 10번 요청

        for i in range(num_requests):
            start_index = i * 10 + 1
            url = "https://www.googleapis.com/customsearch/v1"
            params = {
                'key': api_key, 'cx': pse_id,
                'q': f'"{ticker}" stock OR equity financial news OR corporate guidance',
                'sort': 'date', 'num': 10, 'start': start_index
            }
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                search_results = response.json().get('items', [])
                if not search_results:
                    break # 더 이상 결과가 없으면 중단
                
                for item in search_results:
                    all_articles.append({"source": item['displayLink'], "title": item['title'], "url": item['link'], "snippet": item.get('snippet', '')})
                
                time.sleep(0.1) # API 요청 간 짧은 지연시간

            except requests.exceptions.RequestException as e:
                logger.error(f"Google Search API request failed for {ticker} at page {i+1}: {e}")
                break
        
        logger.info(f"Fetched a total of {len(all_articles)} articles for {ticker}.")
        return all_articles

    def _analyze_searched_news_with_llm(self, articles: list, ticker: str) -> list:
        """
        CHANGED: Re-engineered prompt for ultra-sensitive scoring on a -1000 to 1000 scale.
        """
        if not self.llm_model or not articles:
            return []

        articles_text = "\n\n".join([f"Article {i+1}:\nTitle: {article['title']}\nSnippet: {article['snippet']}" for i, article in enumerate(articles)])
        
        prompt = f"""
        You are a hyper-sensitive quantitative analysis engine. Your task is to analyze financial news for the ticker "{ticker}" with extreme precision.
        
        **Instructions:**
        1.  **Score Range:** Assign a `sentiment_score` for each article on a scale from **-1000 (catastrophic news)** to **+1000 (breakthrough news)**.
        2.  **High Sensitivity:** DO NOT default to neutral scores like 0. A slightly positive earnings report might be a +150, while a major new product launch could be a +750. A minor legal issue might be a -120, while a failed clinical trial could be a -950. Capture every nuance.
        3.  **Quant-Ready Output:** Provide two other quantitative metrics: `impact_rating` (1-5 scale of market-moving potential) and `novelty_score` (1-5 scale of how new or surprising the information is).
        4.  **Strict JSON Output:** Return ONLY a JSON array of objects. The array must have exactly {len(articles)} objects. Each object MUST contain these keys: `sentiment_score` (integer), `impact_rating` (integer), `novelty_score` (integer).

        Analyze these articles now:
        {articles_text}
        """
        try:
            response = self.llm_model.generate_content(prompt, generation_config={"max_output_tokens": 8192, "temperature": 0.0})
            analysis_results = json.loads(response.text.strip().lstrip("```json").rstrip("```"))
            
            for i, article in enumerate(articles):
                if i < len(analysis_results):
                    article.update(analysis_results[i])
            return articles
        except Exception as e:
            logger.error(f"Failed to perform high-sensitivity analysis with LLM: {e}")
            return []
            
    def run_analysis(self, top_n_topics=50, top_n_tickers=5, sentiment_interval='D', specific_tickers=None):
        logger.info("Starting news data analysis (Version 3.0: Ultra-Sensitive Quantitative).")
        analysis_results = {}
        if self.df.empty: return {}

        # --- Base Data Aggregation (Scraped + LLM) ---
        all_ticker_data = defaultdict(lambda: {"mentions": 0, "sentiment_scores": [], "impact_ratings": [], "novelty_scores": []})

        # 1. Process originally scraped data
        if 'ticker_sentiment' in self.df.columns and self.df['ticker_sentiment'].notna().any():
            exploded_tickers = self.df.dropna(subset=['ticker_sentiment']).explode('ticker_sentiment')
            for _, row in exploded_tickers.iterrows():
                ts_info = row['ticker_sentiment']
                if isinstance(ts_info, dict) and ts_info.get('ticker'):
                    ticker = ts_info['ticker']
                    # Convert original -1 to 1 score to the new -1000 to 1000 scale
                    scaled_score = float(ts_info.get('ticker_sentiment_score', 0.0)) * 1000
                    all_ticker_data[ticker]["mentions"] += 1
                    all_ticker_data[ticker]["sentiment_scores"].append(scaled_score)

        # 2. Augment with new, highly sensitive LLM analysis
        target_tickers = specific_tickers if specific_tickers else [t for t, d in sorted(all_ticker_data.items(), key=lambda item: item[1]['mentions'], reverse=True)[:top_n_tickers]]
        
        logger.info("--- Starting Ultra-Sensitive LLM News Augmentation ---")
        for ticker in target_tickers:
            logger.info(f"Fetching and analyzing up to 100 news articles for {ticker}...")
            searched_articles = self._search_google_for_news(ticker)
            if searched_articles:
                analyzed_articles = self._analyze_searched_news_with_llm(searched_articles, ticker)
                if analyzed_articles:
                    # Add raw analysis to a new key for full data transparency
                    all_ticker_data[ticker]['llm_analyzed_news'] = analyzed_articles
                    for article in analyzed_articles:
                        all_ticker_data[ticker]['mentions'] += 1
                        all_ticker_data[ticker]['sentiment_scores'].append(article.get('sentiment_score', 0))
                        all_ticker_data[ticker]['impact_ratings'].append(article.get('impact_rating', 0))
                        all_ticker_data[ticker]['novelty_scores'].append(article.get('novelty_score', 0))

        # --- Final Calculation & Structuring ---
        final_ticker_analysis = {}
        for ticker in target_tickers:
            data = all_ticker_data[ticker]
            scores = data['sentiment_scores']
            impacts = data['impact_ratings']
            novelties = data['novelty_scores']
            
            if not scores: continue # Skip tickers with no data

            # REMOVED: All categorical labels. Only quantitative metrics remain.
            final_ticker_analysis[ticker] = {
                "total_mentions": data['mentions'],
                "average_sentiment_score": sum(scores) / len(scores) if scores else 0,
                "sentiment_std_dev": pd.Series(scores).std() if len(scores) > 1 else 0, # 감성 점수 변동성
                "average_impact_rating": sum(impacts) / len(impacts) if impacts else 0,
                "average_novelty_score": sum(novelties) / len(novelties) if novelties else 0,
                "llm_analyzed_news_count": len(impacts) # LLM이 분석한 기사 수
            }
        
        analysis_results['quantitative_ticker_analysis'] = final_ticker_analysis
        logger.info("Quantitative analysis finished.")
        return analysis_results