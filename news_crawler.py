from bs4 import BeautifulSoup
import requests
import pymysql
import os
from datetime import datetime
import uuid
from dotenv import load_dotenv
import time
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 환경변수 로드
load_dotenv(verbose=True)

# RDS 환경설정 -> 추후 수정 예정
RDS_ENDPOINT = os.getenv('RDS_ENDPOINT')
RDS_PORT_NUM = int(os.getenv('RDS_PORT_NUM'))
RDS_USERNAME = os.getenv('RDS_USERNAME')
RDS_PASSWORD = os.getenv('RDS_PASSWORD')
RDS_DATABASE_NAME = os.getenv('RDS_DATABASE_NAME')

# 크롤링 설정 -> 연합 뉴스의 금융 뉴스를 불러오도록 했습니다
BASE_URL = "https://www.yna.co.kr"
FINANCE_URL = f"{BASE_URL}/economy/finance"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def create_connection():
    """RDS 연결 생성"""
    try:
        connection = pymysql.connect(
            host=os.getenv('RDS_ENDPOINT'),
            port=int(os.getenv('RDS_PORT_NUM')),
            user=os.getenv('RDS_USERNAME'), 
            password=os.getenv('RDS_PASSWORD'),
            database=os.getenv('RDS_DATABASE_NAME'),
            charset='utf8mb4'
        )
        return connection
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return None

def get_news_urls():
    """연합뉴스 금융 섹션의 뉴스 URL 수집"""
    try:
        response = requests.get(FINANCE_URL, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        
        news_urls = []
        # 뉴스 리스트 선택자 수정
        articles = soup.select("div.list-type038 ul li a[href*='/view/']")
        
        for article in articles:
            # URL 수정: BASE_URL 중복 제거
            url = article['href']
            if not url.startswith('http'):
                url = BASE_URL + url
            news_urls.append(url)
            
        logger.info(f"Collected {len(news_urls)} news URLs")
        return news_urls

    except Exception as e:
        logger.error(f"Error collecting news URLs: {str(e)}")
        return []

def parse_news(url):
    """개별 뉴스 페이지 파싱"""
    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 제목 추출
        title = soup.select_one("div.content03 header.title-article01 h1.tit")
        if not title:
            raise ValueError("Title not found")
        title = title.text.strip()
        
        # 본문 추출
        content_elements = soup.select("article.story-news.article p")
        if not content_elements:
            raise ValueError("Content not found")
        
        # 각 p 태그의 텍스트를 리스트로 모으고 공백 제거 후 합치기
        content = ' '.join([p.text.strip() for p in content_elements if p.text.strip()])
        
        # 본문이 비어있는 경우
        if not content:
            raise ValueError("Content is empty")
        
        # 발행시간 추출
        date_element = soup.select_one("p.update-time")
        if not date_element:
            raise ValueError("publish date not found")
        date_str = date_element.text.strip()
        
        # 송고 시간<- 텍스트 제거하고 날짜 파싱
        date_str = date_str.replace("송고시간", "").strip()
        try:
            publish_time = datetime.strptime(
                date_str,
                "%Y-%m-%d %H:%M"
            ).strftime('%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            logger.error(f"날짜 파싱 오류: {date_str}")
            raise e
        
        news_data = {
            'id': str(uuid.uuid4()),
            'title': title,
            'content': content,
            'original_url': url,
            'publish_time': publish_time
        }
        
        logger.info(f"Successfully parsed news: {title}")
        return news_data

    except Exception as e:
        logger.error(f"Error parsing {url}: {str(e)}")
        return None

def save_news_to_rds(connection, news_data):
    """뉴스 데이터를 RDS에 저장"""
    try:
        with connection.cursor() as cursor:
            # ERD에 맞춘 news 테이블 insert 쿼리
            insert_query = """
            INSERT INTO news (
                news_title,
                news_content,
                original_url,
                crawl_time,
                publish_time
            ) VALUES (%s, %s, %s, %s, %s)
            """
            
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            cursor.execute(insert_query, (
                news_data['title'],
                news_data['content'],
                news_data['original_url'],
                current_time,
                news_data['publish_time']
            ))
            
            connection.commit()
            logger.info(f"Successfully saved news: {news_data['title']}")
            
    except Exception as e:
        logger.error(f"Error saving news to database: {str(e)}")
        connection.rollback()

def clean_old_news(connection, days_to_keep=30):
    """오래된 뉴스 데이터 삭제"""
    try:
        with connection.cursor() as cursor:
            delete_query = """
            DELETE FROM news 
            WHERE publish_time < DATE_SUB(NOW(), INTERVAL %s DAY)
            """
            cursor.execute(delete_query, (days_to_keep,))
            connection.commit()
            logger.info(f"Cleaned up news older than {days_to_keep} days")
    except Exception as e:
        logger.error(f"Error cleaning old news: {str(e)}")
        connection.rollback()

def main():
    """메인 실행 함수"""
    logger.info("Starting news crawler...")
    
    # DB 연결
    connection = create_connection()
    if not connection:
        return
    
    try:
        # 뉴스 URL 수집
        news_urls = get_news_urls()
        
        # 각 뉴스 처리
        for url in news_urls:
            try:
                news_data = parse_news(url)
                if news_data:
                    save_news_to_rds(connection, news_data)
                time.sleep(1)  # 크롤링 간격 조절
                
            except Exception as e:
                logger.error(f"Error processing news {url}: {str(e)}")
                continue
        
        # 오래된 뉴스 정리
        clean_old_news(connection)
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        
    finally:
        connection.close()
        logger.info("Crawler finished")

if __name__ == "__main__":
    main()