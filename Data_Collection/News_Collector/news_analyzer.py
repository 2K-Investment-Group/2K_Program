import pandas as pd
import logging
import os
import json
import requests
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
                self.llm_model = genai.GenerativeModel('gemini-1.5-flash')
                logger.info("Gemini 1.5 Flash model initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini LLM model: {e}", exc_info=True)

    def _get_sentiment_label(self, score: float) -> str:
        if score is None: return "Neutral"
        if score >= 0.35: return "Bullish"
        elif score >= 0.15: return "Somewhat-Bullish"
        elif score < -0.35: return "Bearish"
        elif score < -0.15: return "Somewhat-Bearish"
        else: return "Neutral"

    def _search_google_for_news(self, ticker: str, num_articles=5) -> list:
        api_key = os.getenv("GOOGLE_API_KEY")
        pse_id = os.getenv("PSE_ID")
        if not api_key or not pse_id:
            logger.warning("GOOGLE_API_KEY or PSE_ID not found. Google search is skipped.")
            return []

        url = "https://www.googleapis.com/customsearch/v1"
        params = {
            'key': api_key,
            'cx': pse_id,
            'q': f'"{ticker}" stock financial news',
            'sort': 'date',
            'num': num_articles
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            search_results = response.json().get('items', [])
            return [
                {"source": item['displayLink'], "title": item['title'], "url": item['link'], "snippet": item.get('snippet', '')}
                for item in search_results
            ]
        except requests.exceptions.RequestException as e:
            logger.error(f"Google Search API request failed for {ticker}: {e}")
            return []

    def _analyze_searched_news_with_llm(self, articles: list) -> list:
        if not self.llm_model or not articles:
            return []

        articles_text = "\n\n".join([
            f"Article {i+1}:\nTitle: {article['title']}\nSnippet: {article['snippet']}"
            for i, article in enumerate(articles)
        ])
        
        # 프롬프트에서 요청하는 레이블을 더 명확하게 지정하여 일관성을 높입니다.
        prompt = f"""
        Analyze the sentiment of the following news articles. For each article, provide a sentiment score and a sentiment label.
        The sentiment_label MUST be one of the following exact strings: ["Bullish", "Somewhat-Bullish", "Neutral", "Somewhat-Bearish", "Bearish"].
        Return the result ONLY as a JSON array of objects. Each object must have "sentiment_score" (float, -1.0 to 1.0) and "sentiment_label".
        The array must have exactly {len(articles)} objects in the same order as the input articles.

        Articles:
        {articles_text}
        """
        try:
            response = self.llm_model.generate_content(prompt, generation_config={"max_output_tokens": 8192})
            analysis_results = json.loads(response.text.strip().lstrip("```json").rstrip("```"))
            
            for i, article in enumerate(articles):
                if i < len(analysis_results):
                    article.update(analysis_results[i])
            return articles
        except Exception as e:
            logger.error(f"Failed to analyze searched news with LLM: {e}")
            return []
            
    def run_analysis(self, top_n_topics=50, top_n_tickers=5, sentiment_interval='D', specific_tickers=None):
        logger.info("Starting news data analysis (Version 1.1: KeyError Hotfix).")
        analysis_results = {}
        if self.df.empty: return {}

        all_ticker_data = {}
        if 'ticker_sentiment' in self.df.columns and self.df['ticker_sentiment'].notna().any():
            exploded_tickers = self.df.dropna(subset=['ticker_sentiment']).explode('ticker_sentiment')
            for _, row in exploded_tickers.iterrows():
                ts_info = row['ticker_sentiment']
                if isinstance(ts_info, dict):
                    ticker = ts_info.get('ticker')
                    if not ticker: continue
                    if ticker not in all_ticker_data:
                        all_ticker_data[ticker] = {"mentions": 0, "sentiment_scores_list": [], "sentiment_distribution": {'Bullish': 0, 'Somewhat-Bullish': 0, 'Neutral': 0, 'Somewhat-Bearish': 0, 'Bearish': 0}}
                    all_ticker_data[ticker]["mentions"] += 1
                    all_ticker_data[ticker]["sentiment_scores_list"].append(float(ts_info.get('ticker_sentiment_score', 0.0)))
                    all_ticker_data[ticker]["sentiment_distribution"][ts_info.get('ticker_sentiment_label', 'Neutral')] += 1

        top_ticker_sentiment_results = {}
        target_tickers = specific_tickers if specific_tickers else [t for t, d in sorted(all_ticker_data.items(), key=lambda item: item[1]['mentions'], reverse=True)[:top_n_tickers]]
        for ticker in target_tickers:
            if ticker in all_ticker_data:
                data = all_ticker_data[ticker]
                avg_score = sum(data["sentiment_scores_list"]) / len(data["sentiment_scores_list"]) if data["sentiment_scores_list"] else 0
                top_ticker_sentiment_results[ticker] = {"mentions": data["mentions"], "average_sentiment_score": avg_score, "overall_sentiment_label": self._get_sentiment_label(avg_score), "sentiment_distribution": data["sentiment_distribution"].copy()} # Use copy to avoid reference issues
        analysis_results['top_ticker_sentiment'] = top_ticker_sentiment_results
        
        if top_ticker_sentiment_results:
            logger.info("--- Starting LLM-powered News Augmentation ---")
            for ticker, data in top_ticker_sentiment_results.items():
                logger.info(f"Searching additional news for {ticker}...")
                searched_articles = self._search_google_for_news(ticker)
                if searched_articles:
                    analyzed_articles = self._analyze_searched_news_with_llm(searched_articles)
                    if analyzed_articles:
                        data['llm_found_news'] = analyzed_articles
                        for article in analyzed_articles:
                            # ⭐️⭐️⭐️ THIS IS THE FIX ⭐️⭐️⭐️
                            # This one-line change makes the code robust against any label from the LLM.
                            label = article.get('sentiment_label', 'Neutral')
                            data['sentiment_distribution'][label] = data['sentiment_distribution'].get(label, 0) + 1
                            
                            # Also update mentions and score list for recalculation
                            data['mentions'] += 1
                            if 'sentiment_scores_list' not in all_ticker_data[ticker]: all_ticker_data[ticker]['sentiment_scores_list'] = [] # Ensure list exists
                            all_ticker_data[ticker]['sentiment_scores_list'].append(article.get('sentiment_score', 0.0))
                        
                        # Recalculate average score
                        updated_scores = all_ticker_data[ticker]['sentiment_scores_list']
                        new_avg_score = sum(updated_scores) / len(updated_scores) if updated_scores else 0
                        data['average_sentiment_score'] = new_avg_score
                        data['overall_sentiment_label'] = self._get_sentiment_label(new_avg_score)
                        logger.info(f"Successfully augmented data for {ticker} with {len(analyzed_articles)} new articles.")

        return analysis_results