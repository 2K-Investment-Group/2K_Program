import pandas as pd
import logging
import os
import google.generativeai as genai  

logger = logging.getLogger(__name__)

class NewsAnalyzer:
    def __init__(self, dataframe: pd.DataFrame):
        """
        Initializes the analyzer with a pre-processed pandas DataFrame.
        Also initializes the Gemini LLM client if the API key is available.
        """
        self.df = dataframe
        logger.info(f"NewsAnalyzer initialized with a DataFrame of shape {self.df.shape}.")

        self.llm_model = None
        try:
            gemini_api_key = os.getenv("GEMINI_API_KEY")
            if not gemini_api_key:
                logger.warning("GEMINI_API_KEY environment variable not found. LLM analysis will be skipped.")
            else:
                genai.configure(api_key=gemini_api_key)
                self.llm_model = genai.GenerativeModel('gemini-1.5-flash')
                logger.info("Gemini 1.5 Flash model initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini LLM model: {e}", exc_info=True)
            self.llm_model = None
    
    def _get_sentiment_label(self, score: float) -> str:
        """Categorizes a sentiment score into a human-readable label."""
        if score is None:
            return "Neutral"
        if score >= 0.35:
            return "Bullish"
        elif score >= 0.15:
            return "Somewhat-Bullish"
        elif score < -0.35:
            return "Bearish"
        elif score < -0.15:
            return "Somewhat-Bearish"
        else:
            return "Neutral"

    def _get_llm_summary_for_ticker(self, ticker: str, top_n_articles=5) -> str:
        """
        ADDED: 특정 티커에 대한 뉴스 기사들을 모아 LLM에게 요약 및 인사이트를 요청합니다.
        """
        if not self.llm_model:
            return "LLM model is not available."

        if 'ticker_sentiment' not in self.df.columns or self.df['ticker_sentiment'].isna().all():
            return "No ticker sentiment data available for LLM analysis."
            
        temp_df = self.df.dropna(subset=['ticker_sentiment']).explode('ticker_sentiment')
        ticker_articles_df = temp_df[temp_df['ticker_sentiment'].apply(lambda x: isinstance(x, dict) and x.get('ticker') == ticker)]

        recent_articles = ticker_articles_df.sort_values(by='published_at', ascending=False).head(top_n_articles)

        if recent_articles.empty:
            return f"No relevant articles found for ticker {ticker} to generate a summary."

        news_texts = "\n\n---\n\n".join(
            f"Title: {row['title']}\nSummary: {row.get('summary') or 'Not available.'}"
            for _, row in recent_articles.iterrows()
        )

        prompt = f"""
        You are a highly experienced Wall Street analyst providing a report to a key client.
        Based *only* on the provided news articles about the ticker "{ticker}", generate a concise, insightful summary in Korean.

        Your summary must include:
        1.  **Overall Sentiment**: What is the dominant sentiment (e.g., positive, negative, mixed) and why?
        2.  **Key Drivers**: Identify the main factors or events driving the news (e.g., earnings reports, product launches, market competition, regulatory news).
        3.  **Potential Outlook**: Based on the articles, what is the potential short-term outlook for the stock?

        Do not use any information beyond the articles provided below. Be objective and professional.

        Provided News Articles:
        ---
        {news_texts}
        """

        try:
            response = self.llm_model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            logger.error(f"An error occurred during Gemini API call for ticker {ticker}: {e}", exc_info=True)
            return f"Error: Failed to generate LLM summary for {ticker}."

    def run_analysis(self, top_n_topics=50, top_n_tickers=5, sentiment_interval='D', specific_tickers=None):
        """
        Performs various analyses, now including LLM-generated insights at the end.
        """
        logger.info("Starting news data analysis.")
        analysis_results = {}

        if self.df.empty:
            logger.warning("No data to analyze. DataFrame is empty.")
            return {}

        if 'published_at' in self.df.columns and 'sentiment_score' in self.df.columns:
            temp_df = self.df.set_index('published_at')
            sentiment_over_time = temp_df['sentiment_score'].resample(sentiment_interval).mean().dropna()
            analysis_results['sentiment_over_time'] = {str(k.date()): v for k, v in sentiment_over_time.items()}
            logger.info(f"Sentiment over time calculated for interval '{sentiment_interval}'.")

        if 'topics' in self.df.columns and self.df['topics'].notna().any():
            topic_scores = {}
            exploded_topics = self.df.dropna(subset=['topics']).explode('topics')
            for _, row in exploded_topics.iterrows():
                topic_info = row['topics']
                if isinstance(topic_info, dict):
                    topic = topic_info.get('topic')
                    try:
                        relevance = float(topic_info.get('relevance_score', 0.0))
                        if topic:
                            topic_scores[topic] = topic_scores.get(topic, 0) + relevance
                    except (ValueError, TypeError):
                        continue
            sorted_topics = sorted(topic_scores.items(), key=lambda item: item[1], reverse=True)
            analysis_results['top_topics'] = dict(sorted_topics[:top_n_topics])
            logger.info(f"Top {top_n_topics} topics identified.")
        else:
            analysis_results['top_topics'] = {}

        if 'api_source' in self.df.columns and 'sentiment_score' in self.df.columns:
            api_source_sentiment = self.df.groupby('api_source')['sentiment_score'].mean().to_dict()
            analysis_results['api_source_sentiment'] = api_source_sentiment
            logger.info("Average sentiment by API source calculated.")
        else:
            analysis_results['api_source_sentiment'] = {}

        top_ticker_sentiment_results = {}
        if 'ticker_sentiment' in self.df.columns and self.df['ticker_sentiment'].notna().any():
            all_ticker_data = {}
            exploded_tickers = self.df.dropna(subset=['ticker_sentiment']).explode('ticker_sentiment')
            for _, row in exploded_tickers.iterrows():
                ts_info = row['ticker_sentiment']
                if isinstance(ts_info, dict):
                    ticker = ts_info.get('ticker')
                    try:
                        score = float(ts_info.get('ticker_sentiment_score', 0.0))
                        label = ts_info.get('ticker_sentiment_label', 'Neutral')
                        if ticker:
                            if ticker not in all_ticker_data:
                                all_ticker_data[ticker] = {
                                    "mentions": 0, "sentiment_scores_list": [],
                                    "sentiment_distribution": {'Bullish': 0, 'Somewhat-Bullish': 0, 'Neutral': 0, 'Somewhat-Bearish': 0, 'Bearish': 0}
                                }
                            all_ticker_data[ticker]["mentions"] += 1
                            all_ticker_data[ticker]["sentiment_scores_list"].append(score)
                            all_ticker_data[ticker]["sentiment_distribution"][label] += 1
                    except (ValueError, TypeError):
                        continue
            target_tickers_for_analysis = []
            if specific_tickers:
                target_tickers_for_analysis = [t for t in specific_tickers if t in all_ticker_data]
            else:
                sorted_by_mentions = sorted(all_ticker_data.items(), key=lambda item: item[1]['mentions'], reverse=True)
                target_tickers_for_analysis = [ticker for ticker, data in sorted_by_mentions[:top_n_tickers]]
            
            if target_tickers_for_analysis:
                logger.info(f"Analyzing ticker sentiment for: {', '.join(target_tickers_for_analysis)}")
                for ticker in target_tickers_for_analysis:
                    data = all_ticker_data[ticker]
                    avg_sentiment = sum(data["sentiment_scores_list"]) / len(data["sentiment_scores_list"])
                    top_ticker_sentiment_results[ticker] = {
                        "mentions": data["mentions"], "average_sentiment_score": avg_sentiment,
                        "overall_sentiment_label": self._get_sentiment_label(avg_sentiment),
                        "sentiment_distribution": data["sentiment_distribution"]
                    }
        analysis_results['top_ticker_sentiment'] = top_ticker_sentiment_results

        if self.llm_model and top_ticker_sentiment_results:
            logger.info("--- Starting LLM-powered Insight Analysis ---")
            llm_insights = {}

            tickers_to_summarize = list(top_ticker_sentiment_results.keys())
            
            for ticker in tickers_to_summarize:
                logger.info(f"Generating LLM summary for ticker: {ticker}")
                summary = self._get_llm_summary_for_ticker(ticker)
                llm_insights[ticker] = summary

            analysis_results['llm_generated_insights'] = llm_insights
            logger.info("LLM analysis finished.")
        elif not self.llm_model:
            logger.info("LLM analysis skipped because the model is not available.")
        
        logger.info("News data analysis finished.")
        return analysis_results