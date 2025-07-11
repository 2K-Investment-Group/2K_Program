import os
import sys
import yaml
import logging
from datetime import datetime, timedelta

# main_collector.py 파일이 있는 디렉토리 (Data_Collection/News_Collector)
current_dir = os.path.dirname(os.path.abspath(__file__))

# 프로젝트 루트 디렉토리를 PYTHONPATH에 추가
# 현재 main_collector.py는 Data_Collection/News_Collector에 있으므로
# 두 단계 위로 올라가야 프로젝트 루트입니다.
project_root = os.path.abspath(os.path.join(current_dir, "..", "..")) 
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 로깅 설정
log_dir = os.path.join(project_root, "logs")
os.makedirs(log_dir, exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, f"main_collector_{timestamp}.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

class MainCollector:
    def __init__(self, config_path="Data_Collection/config/news_sources.yaml"):
        # config_path는 프로젝트 루트 기준입니다.
        self.config = self._load_config(config_path)
        if not self.config:
            raise ValueError(f"Failed to load application configuration from {config_path}")
        
        # 설정 파일에서 output_json_file 값 가져오기 (news_collection 섹션 사용)
        output_file_name = self.config['news_collection'].get('output_json_file', "scraped_api_news_articles.json")

        # 파일 경로 설정 (모두 프로젝트 루트 기준으로 변경)
        self.data_storage_dir = os.path.join(project_root, "Data", "storage")
        os.makedirs(self.data_storage_dir, exist_ok=True)
        self.collected_news_file = os.path.join(
            self.data_storage_dir,
            output_file_name # news_collection의 output_json_file 사용
        )
        
        # news_sources.yaml 자체를 설정으로 사용하므로, news_sources_config_path는 더 이상 필요 없음.
        # 대신, NewsCollector에는 news_api_config 섹션을 직접 전달.
        # self.news_sources_config_path = os.path.join( # 이 라인 삭제 또는 주석 처리
        #     project_root,
        #     self.config['news_collection']['news_sources_config'] 
        # )

        # 시각화 이미지 저장 디렉토리 설정
        self.visualizations_dir = os.path.join(
            project_root,
            self.config['news_analysis'].get('output_image_dir', "visualizations")
        )
        os.makedirs(self.visualizations_dir, exist_ok=True)

    def _load_config(self, config_path: str) -> dict:
        """YAML 설정 파일을 로드합니다."""
        # 이 함수 내에서는 이미 project_root가 설정되어 있으므로 project_root를 사용합니다.
        abs_config_path = os.path.join(project_root, config_path)
        try:
            with open(abs_config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"Successfully loaded configuration from {abs_config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {abs_config_path}")
            return None
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML configuration file {abs_config_path}: {e}")
            return None

    def run_news_collection(self):
        """뉴스 수집을 실행합니다."""
        if not self.config['news_collection']['enabled']:
            logger.info("News collection is disabled in news_sources.yaml. Skipping.")
            return

        logger.info("--- Starting News Collection ---")
        try:
            # news_scraper.py는 현재 Data_Collection/News_Collector/ 에 있으므로
            # from news_scraper import NewsCollector 형태로 임포트
            from news_scraper import NewsCollector 
            
            # NewsCollector에 news_api_config 섹션 전체를 전달
            collector = NewsCollector(
                api_config=self.config['news_api_config'], 
                output_file_path=self.collected_news_file
            )
            collector.run_collection()
            
            logger.info("News collection completed successfully.")
        except ImportError as e:
            logger.error(f"Failed to import NewsCollector from news_scraper.py. Ensure the class name is correct and file is in Data_Collection/News_Collector/. Error: {e}")
        except Exception as e:
            logger.error(f"An error occurred during news collection: {e}", exc_info=True)


    def run_news_processing(self):
        """뉴스 데이터 처리를 실행합니다."""
        if not self.config['news_processing']['enabled']:
            logger.info("News processing is disabled in news_sources.yaml. Skipping.")
            return

        logger.info("--- Starting News Processing ---")
        try:
            from news_processor import NewsProcessor
            
            # NewsProcessor 초기화 시 input_file_path만 전달
            processor = NewsProcessor(input_file_path=self.collected_news_file)
            processor.run_processing() # 함수명 일관성 유지: run_analysis 대신 run_processing
            
            logger.info("News processing completed successfully.")
        except ImportError as e:
            logger.error(f"Failed to import NewsProcessor. Ensure news_processor.py is in Data_Collection/News_Collector/. Error: {e}")
        except Exception as e:
            logger.error(f"An error occurred during news processing: {e}", exc_info=True)


    def run_news_analysis_and_visualization(self):
        """뉴스 데이터 분석 및 시각화를 실행합니다."""
        if not self.config['news_analysis']['enabled']:
            logger.info("News analysis and visualization is disabled in news_sources.yaml. Skipping.")
            return

        logger.info("--- Starting News Analysis and Visualization ---")
        try:
            from news_analyzer import NewsAnalyzer
            
            analyzer_config = self.config['news_analysis']
            analyzer = NewsAnalyzer(
                input_file_path=self.collected_news_file, 
                output_image_dir=self.visualizations_dir
            )
            analyzer.run_analysis(
                top_n_topics_wc=analyzer_config.get('top_n_topics_wordcloud', 50),
                top_n_tickers_pie=analyzer_config.get('top_n_tickers_pie_charts', 5),
                sentiment_interval=analyzer_config.get('sentiment_analysis_interval', 'D')
            )
            
            logger.info("News analysis and visualization completed successfully. Check 'visualizations' folder.")
        except ImportError as e:
            logger.error(f"Failed to import NewsAnalyzer. Ensure news_analyzer.py is in Data_Collection/News_Collector/. Error: {e}")
        except Exception as e:
            logger.error(f"An error occurred during news analysis and visualization: {e}", exc_info=True)

    def run_all(self):
        """모든 단계를 순차적으로 실행합니다."""
        logger.info("--- Starting Main News Collection and Analysis Process ---")
        self.run_news_collection()
        self.run_news_processing()
        self.run_news_analysis_and_visualization()
        logger.info("--- Main News Collection and Analysis Process Finished ---")

if __name__ == "__main__":
    try:
        collector = MainCollector()
        collector.run_all()
    except Exception as e:
        logger.critical(f"Fatal error in MainCollector: {e}", exc_info=True)