import json
import os
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter, defaultdict
from wordcloud import WordCloud
import logging
import matplotlib.font_manager as fm # 폰트 관리자 임포트

# --- Logger setup (Moved to __main__ for better control) ---
# This part will be handled in the main execution block to avoid re-initializing
# the logger with every instance of NewsAnalyzer.

class NewsAnalyzer:
    def __init__(self, input_file_path: str, output_image_dir: str = "visualizations"):
        self.input_file_path = input_file_path
        self.output_image_dir = output_image_dir
        self.df = pd.DataFrame()
        self.logger = logging.getLogger(__name__) # Class-specific logger instance
        
        os.makedirs(self.output_image_dir, exist_ok=True)
        self._set_korean_font() # 폰트 설정 함수 호출
        self.load_data()

    def _set_korean_font(self):
        """
        Matplotlib에서 한글을 표시하기 위한 폰트를 설정합니다.
        여러 폰트를 시도하며, 시스템에 맞는 폰트가 없으면 경고를 남깁니다.
        """
        font_options = ['Malgun Gothic', 'AppleGothic', 'Noto Sans CJK KR']
        for font_name in font_options:
            if any(font_name in f.name for f in fm.fontManager.ttflist):
                plt.rcParams['font.family'] = font_name
                plt.rcParams['axes.unicode_minus'] = False # 마이너스 폰트 깨짐 방지
                self.logger.info(f"Matplotlib font set to {font_name}.")
                return
        self.logger.warning("Could not set Korean font for Matplotlib. Text might appear as squares.")

    def load_data(self):
        """저장된 JSON 파일에서 뉴스 기사를 로드하고 Pandas DataFrame으로 변환합니다."""
        if not os.path.exists(self.input_file_path):
            self.logger.error(f"Error: Input file not found at {self.input_file_path}")
            return

        try:
            with open(self.input_file_path, 'r', encoding='utf-8') as f:
                articles = json.load(f)
            
            # --- 중복 제거 로직 개선 (NewsProcessor와 일관성 유지) ---
            unique_articles_map = {}
            for article in articles:
                url = article.get('url')
                if url:
                    # URL이 있을 경우: 최신 scraped_at 기준으로 중복 제거
                    current_scraped_at = article.get('scraped_at', '0000-00-00T00:00:00')
                    existing_scraped_at = unique_articles_map.get(url, {}).get('scraped_at', '0000-00-00T00:00:00')
                    if url not in unique_articles_map or current_scraped_at > existing_scraped_at:
                        unique_articles_map[url] = article
                else:
                    # URL이 없을 경우: 제목과 발행일시로 대체 키 생성 (엄격한 중복 제거는 아님)
                    fallback_key = f"{article.get('title', 'NO_TITLE')}_{article.get('published_at', 'NO_DATE')}"
                    if fallback_key not in unique_articles_map: # 첫 번째 발견된 기사만 유지
                        unique_articles_map[fallback_key] = article
                        self.logger.warning(f"Article with no URL found, using fallback key: {article.get('title', 'No Title')}")

            self.df = pd.DataFrame(list(unique_articles_map.values()))
            self.logger.info(f"Successfully loaded {len(articles)} articles and retained {len(self.df)} unique/processed articles for analysis.")
            
            # 날짜 컬럼 형식 변환 (Datetime 객체로) - errors='coerce'로 변환 불가 시 NaT 처리
            # infer_datetime_format=True로 다양한 형식 자동 추론 시도
            self.df['published_at'] = pd.to_datetime(self.df['published_at'], errors='coerce', utc=True, infer_datetime_format=True)
            
            # 유효한 published_at이 없는 행 제거
            initial_rows = len(self.df)
            self.df.dropna(subset=['published_at'], inplace=True)
            if len(self.df) < initial_rows:
                self.logger.warning(f"Removed {initial_rows - len(self.df)} rows due to invalid 'published_at' dates.")

            self.df.sort_values(by='published_at', inplace=True)

        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON from {self.input_file_path}: {e}")
            self.df = pd.DataFrame()
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while loading data: {e}", exc_info=True)
            self.df = pd.DataFrame()

    def plot_sentiment_over_time(self, title_suffix: str = "", interval: str = 'D'):
        """
        시간 경과에 따른 전체 시장 감성 변화를 시각화합니다.
        
        Args:
            title_suffix (str): 그래프 제목에 추가할 접미사.
            interval (str): 'D' (일별), 'W' (주별), 'M' (월별) 등 집계 간격.
        """
        # 필수 컬럼 존재 여부 확인
        if self.df.empty or 'published_at' not in self.df.columns or 'ticker_sentiment' not in self.df.columns:
            self.logger.warning("DataFrame is empty or 'published_at'/'ticker_sentiment' columns are missing for time series sentiment analysis. Skipping plot.")
            return

        # 모든 티커 감성 점수를 평탄화
        all_sentiments = []
        for _, row in self.df.iterrows():
            pub_date = row['published_at']
            if pd.isna(pub_date): # NaT 값 건너뛰기
                continue
            for ts in row.get('ticker_sentiment', []):
                try:
                    score = float(ts.get('ticker_sentiment_score', 0.0))
                    all_sentiments.append({'date': pub_date, 'score': score})
                except (ValueError, TypeError):
                    self.logger.warning(f"Invalid ticker_sentiment_score found: {ts.get('ticker_sentiment_score')}")
                    continue
        
        if not all_sentiments:
            self.logger.warning("No valid ticker sentiment data available to plot sentiment over time. Skipping plot.")
            return

        sentiment_df = pd.DataFrame(all_sentiments)
        sentiment_df['date'] = sentiment_df['date'].dt.floor(interval) # 일, 주, 월 단위로 반올림
        
        # 날짜별 평균 감성 점수 계산
        daily_sentiment = sentiment_df.groupby('date')['score'].mean().reset_index()

        plt.figure(figsize=(15, 7))
        sns.lineplot(x='date', y='score', data=daily_sentiment, marker='o', color='skyblue')
        plt.title(f'시간 경과에 따른 평균 시장 감성 변화 {title_suffix}', fontsize=16)
        plt.xlabel('날짜', fontsize=12)
        plt.ylabel('평균 감성 점수', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        
        plot_filename = f'sentiment_over_time_{interval}_{title_suffix.replace(" ", "_")}.png'
        plt.savefig(os.path.join(self.output_image_dir, plot_filename))
        plt.show()
        self.logger.info(f"Generated time series sentiment plot: {plot_filename}")


    def plot_top_topics_wordcloud(self, top_n: int = 50):
        """
        가장 많이 언급된 주제(Topic)를 워드 클라우드로 시각화합니다.
        """
        if self.df.empty or 'topics' not in self.df.columns:
            self.logger.warning("DataFrame is empty or 'topics' column is missing for topic word cloud. Skipping plot.")
            return

        all_topics = Counter()
        for _, row in self.df.iterrows():
            topics_list = row.get('topics', [])
            for topic_info in topics_list:
                topic_name = topic_info.get('topic')
                try:
                    relevance_score = float(topic_info.get('relevance_score', 0.0))
                except (ValueError, TypeError):
                    self.logger.warning(f"Invalid relevance_score for topic '{topic_name}': {topic_info.get('relevance_score')}")
                    relevance_score = 0.0
                
                if topic_name:
                    all_topics[topic_name] += relevance_score # 관련성 점수를 가중치로 사용

        if not all_topics:
            self.logger.warning("No topics found for word cloud. Skipping plot.")
            return

        # WordCloud 폰트 경로 설정 (현재 폰트 설정을 따르거나, 특정 폰트 경로 지정)
        # 시스템에 'Malgun Gothic'이 없을 경우를 대비하여, _set_korean_font에서 설정된 폰트를 사용하거나
        # 사용자에게 직접 폰트 경로를 지정하도록 안내할 수 있습니다.
        # 예시: font_path='/System/Library/Fonts/AppleGothic.ttf' (macOS)
        # 예시: font_path='C:/Windows/Fonts/malgunbd.ttf' (Windows)
        # 또는 현재 Matplotlib의 rcParams에서 설정된 폰트 이름을 가져와 사용할 수 있습니다.
        current_font = plt.rcParams['font.family'][0] if isinstance(plt.rcParams['font.family'], list) else plt.rcParams['font.family']
        # WordCloud는 ttf 파일 경로를 직접 필요로 하므로, 폰트 이름을 기반으로 경로를 찾아야 합니다.
        # 이는 복잡하므로, 가장 일반적인 폰트 파일 이름을 직접 지정하는 것이 현실적입니다.
        # 이 예시에서는 'malgun.ttf'를 직접 사용합니다. 사용자에게 해당 폰트 파일이 필요하다고 안내해야 합니다.
        wordcloud_font_path = None
        for font_path_obj in fm.fontManager.ttflist:
            if current_font in font_path_obj.name:
                wordcloud_font_path = font_path_obj.fname
                break
        
        if not wordcloud_font_path:
            self.logger.warning(f"Could not find font file for WordCloud using '{current_font}'. Please ensure 'malgun.ttf' or a suitable Korean font is in your system or specified path.")
            # 대체 폰트 경로 지정 또는 경고 후 함수 종료
            # 예시: wordcloud_font_path = 'C:/Windows/Fonts/malgun.ttf' 
            # 이 코드는 WordCloud가 Matplotlib 폰트 설정과 다르게 실제 파일 경로를 필요로 하기 때문에
            # 사용자의 시스템 환경에 맞는 폰트 파일 경로를 명시해야 합니다.
            # 현재는 가장 일반적인 'malgun.ttf'를 가정하고 있습니다.
            wordcloud_font_path = 'malgun.ttf' # WordCloud가 기본 폰트 경로에서 찾을 수 있도록 시도
            

        wordcloud = WordCloud(width=800, height=400, background_color='white',
                              font_path=wordcloud_font_path,
                              collocations=False).generate_from_frequencies(all_topics)
        
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.title('뉴스 기사 주요 주제 워드 클라우드', fontsize=16)
        plt.tight_layout(pad=0)
        
        plot_filename = 'top_topics_wordcloud.png'
        plt.savefig(os.path.join(self.output_image_dir, plot_filename))
        plt.show()
        self.logger.info(f"Generated top topics word cloud: {plot_filename}")

    def plot_api_source_sentiment_comparison(self):
        """
        각 API 소스별 평균 감성 점수를 비교하여 시각화합니다.
        """
        if self.df.empty or 'api_source' not in self.df.columns or 'ticker_sentiment' not in self.df.columns:
            self.logger.warning("DataFrame is empty or 'api_source'/'ticker_sentiment' columns are missing for source sentiment comparison. Skipping plot.")
            return

        source_sentiments = defaultdict(list)
        for _, row in self.df.iterrows():
            api_source = row.get('api_source')
            if not api_source:
                self.logger.debug(f"Skipping article with no 'api_source': {row.get('title', 'No Title')}")
                continue
            
            article_sentiment_scores = [float(ts.get('ticker_sentiment_score', 0.0)) 
                                        for ts in row.get('ticker_sentiment', []) 
                                        if isinstance(ts.get('ticker_sentiment_score'), (int, float, str))] # 유효한 숫자 형식만 포함
            
            if article_sentiment_scores:
                source_sentiments[api_source].append(sum(article_sentiment_scores) / len(article_sentiment_scores))
            else: 
                # 티커 감성 정보가 없는 기사도 중립으로 간주하여 포함할 수 있음 (분석 목적에 따라 선택)
                # 현재는 해당 소스에서 감성 데이터가 없는 경우를 위한 평균 계산에서 제외될 수 있음
                self.logger.debug(f"Article from '{api_source}' has no valid ticker sentiment. Skipping from source sentiment average.")

        avg_source_sentiment = {source: sum(scores) / len(scores) for source, scores in source_sentiments.items() if scores}
        
        if not avg_source_sentiment:
            self.logger.warning("No valid sentiment data available for API source comparison. Skipping plot.")
            return

        sources = list(avg_source_sentiment.keys())
        scores = list(avg_source_sentiment.values())

        plt.figure(figsize=(10, 6))
        sns.barplot(x=sources, y=scores, palette='viridis')
        plt.title('API 소스별 평균 감성 점수', fontsize=16)
        plt.xlabel('API 소스', fontsize=12)
        plt.ylabel('평균 감성 점수', fontsize=12)
        plt.ylim(-0.5, 0.5) # 감성 점수 범위 (-1 ~ 1)
        plt.axhline(0, color='gray', linestyle='--', linewidth=0.8) # 0점 기준선
        plt.tight_layout()
        
        plot_filename = 'api_source_sentiment_comparison.png'
        plt.savefig(os.path.join(self.output_image_dir, plot_filename))
        plt.show()
        self.logger.info(f"Generated API source sentiment comparison plot: {plot_filename}")

    def plot_top_ticker_sentiment_pie_charts(self, top_n: int = 5):
        """
        가장 많이 언급된 티커별 감성 분포를 파이 차트로 시각화합니다.
        """
        if self.df.empty or 'ticker_sentiment' not in self.df.columns:
            self.logger.warning("DataFrame is empty or 'ticker_sentiment' column is missing for ticker sentiment analysis. Skipping plot.")
            return

        ticker_mentions = Counter()
        ticker_sentiment_labels = defaultdict(Counter)

        for _, row in self.df.iterrows():
            for ts in row.get('ticker_sentiment', []):
                ticker = ts.get('ticker')
                sentiment_label = ts.get('ticker_sentiment_label', 'Unknown')
                if ticker:
                    ticker_mentions[ticker] += 1
                    ticker_sentiment_labels[ticker][sentiment_label] += 1
        
        if not ticker_mentions:
            self.logger.warning("No ticker sentiment data available for pie charts. Skipping plot.")
            return

        sorted_tickers = ticker_mentions.most_common(top_n)

        # 서브플롯 그리드 크기 계산
        num_plots = len(sorted_tickers)
        if num_plots == 0:
            self.logger.info("No tickers to plot pie charts for after filtering.")
            return
        
        # 행 개수: 각 행에 2개씩 배치. 홀수 개면 마지막 행에 1개
        num_rows = (num_plots + 1) // 2 
        plt.figure(figsize=(15, 6 * num_rows)) # 각 차트당 높이 6인치 예상

        for i, (ticker, count) in enumerate(sorted_tickers):
            labels = list(ticker_sentiment_labels[ticker].keys())
            sizes = list(ticker_sentiment_labels[ticker].values())
            
            if not labels: # 감성 라벨이 없는 경우 (매우 드물겠지만)
                self.logger.warning(f"No sentiment labels for ticker {ticker}. Skipping pie chart for this ticker.")
                continue

            ax = plt.subplot(num_rows, 2, i + 1) # num_rows 행, 2열 그리드
            # 폰트 사이즈 조정: 파이 차트의 텍스트가 겹치지 않도록
            wedges, texts, autotexts = ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, textprops={'fontsize': 10}, pctdistance=0.85)
            # 퍼센트 텍스트를 더 명확하게 표시하기 위한 로직 (옵션)
            for autotext in autotexts:
                autotext.set_color('white') # 퍼센트 텍스트 색상 변경
                autotext.set_fontsize(9) # 퍼센트 텍스트 폰트 크기 변경
            
            ax.axis('equal') # 원형 유지
            ax.set_title(f'{ticker} 감성 분포 (언급: {count}회)', fontsize=14)
        
        plt.tight_layout()
        plot_filename = f'top_{top_n}_ticker_sentiment_pie_charts.png'
        plt.savefig(os.path.join(self.output_image_dir, plot_filename))
        plt.show()
        self.logger.info(f"Generated top {top_n} ticker sentiment pie charts: {plot_filename}")


    def run_analysis(self, top_n_topics_wc: int = 50, top_n_tickers_pie: int = 5, sentiment_interval: str = 'D'):
        """전체 분석 및 시각화 프로세스를 실행합니다."""
        self.logger.info("Starting news data analysis and visualization...")
        
        if self.df.empty:
            self.logger.error("No data loaded into DataFrame. Analysis aborted.")
            return

        self.plot_sentiment_over_time(interval=sentiment_interval)
        self.plot_top_topics_wordcloud(top_n=top_n_topics_wc)
        self.plot_api_source_sentiment_comparison()
        self.plot_top_ticker_sentiment_pie_charts(top_n=top_n_tickers_pie)
        
        self.logger.info(f"News data analysis and visualization finished. Check '{self.output_image_dir}' folder.")

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
            logging.FileHandler(os.path.join(log_dir, f"news_analyzer_{timestamp}.log")),
            logging.StreamHandler(),
        ],
    )
    # Get logger for this module
    main_logger = logging.getLogger(__name__)
    main_logger.info("News Analyzer script started.")

    # 데이터가 저장된 파일 경로
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
    data_file_path = os.path.join(base_dir, "Data", "storage", "scraped_api_news_articles.json")
    
    # 시각화 이미지를 저장할 디렉토리 (프로젝트 루트 아래)
    output_viz_dir = os.path.join(base_dir, "visualizations")

    analyzer = NewsAnalyzer(data_file_path, output_viz_dir)
    analyzer.run_analysis(top_n_topics_wc=50, top_n_tickers_pie=5, sentiment_interval='D')