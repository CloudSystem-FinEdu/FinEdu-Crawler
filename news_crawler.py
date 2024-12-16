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
        logging.FileHandler('/app/logs/crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# MySQL 환경변수 설정
DB_HOST = os.getenv('MYSQL_HOST', 'mysql')
DB_PORT = int(os.getenv('MYSQL_PORT', 3306))
DB_USER = os.getenv('MYSQL_USER', 'finedu_user')
DB_PASSWORD = os.getenv('MYSQL_PASSWORD', 'finedu_password')
DB_NAME = os.getenv('MYSQL_DATABASE', 'finedu')

# 크롤링 설정
BASE_URL = "https://www.yna.co.kr"
FINANCE_URL = f"{BASE_URL}/economy/finance"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def create_connection():
    """MySQL 데이터베이스 연결 생성"""
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4'
        )
        logger.info(f"MySQL 연결 성공 at {DB_HOST}")
        return connection
    except Exception as e:
        logger.error(f"데이터베이스 연결 오류: {str(e)}")
        return None


def get_news_urls():
    try:
        response = requests.get(FINANCE_URL, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        
        # set을 사용하여 중복 URL 제거
        news_urls = set()
        articles = soup.select("div.list-type038 ul li a[href*='/view/']")
        
        for article in articles:
            url = article['href']
            if not url.startswith('http'):
                url = BASE_URL + url
            news_urls.add(url)  # docker log에서 중복 기사 저장 감지 -> set에 추가하여 중복 제거
            
        logger.info(f"{len(news_urls)}개의 고유한 뉴스 URL을 저장했습니다")
        return list(news_urls)  # list로 변환하여 반환

    except Exception as e:
        logger.error(f"뉴스 URL을 모으는데 실패했습니다: {str(e)}")
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
            raise ValueError("콘텐트를 찾을 수 없습니다")
        
        # 각 p 태그의 텍스트를 리스트로 모으고 공백 제거 후 합치기
        content = ' '.join([p.text.strip() for p in content_elements if p.text.strip()])
        
        # 본문이 비어있는 경우
        if not content:
            raise ValueError("콘텐트가 비어있습니다")
        
        # 발행시간 추출
        date_element = soup.select_one("p.update-time")
        if not date_element:
            raise ValueError("발행일자를 찾을 수 없습니다")
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
        
        logger.info(f"뉴스를 파싱하는데 성공했습니다!: {title}")
        return news_data

    except Exception as e:
        logger.error(f"해당 URL의 뉴스를 파싱하는데 실패했습니다: {url}: {str(e)}")
        return None

def save_news_to_mysql(connection, news_data):
    """뉴스 데이터를 MySQL 데이터베이스에 저장"""
    try:
        with connection.cursor() as cursor:
            # 동일한 URL의 뉴스가 이미 존재하는지 확인 -> 중복 제거!!
            check_query = "SELECT COUNT(*) FROM news WHERE original_url = %s"
            cursor.execute(check_query, (news_data['original_url'],))
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                logger.info(f"이미 저장된 뉴스입니다: {news_data['title']}")
                return
                
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
            logger.info(f"뉴스를 MySQL DB에 저장하는데 성공했습니다: {news_data['title']}")
            
    except Exception as e:
        logger.error(f"MySQL DB에 뉴스를 저장하는데 실패했습니다: {str(e)}")
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
            logger.info(f"{days_to_keep}일 이전에 모은 뉴스를 정리했습니다")
    except Exception as e:
        logger.error(f"오래된 뉴스를 정리하는데 오류가 있었습니다: {str(e)}")
        connection.rollback()

def main():
    """메인 실행 함수"""
    while True:
        try:
            logger.info("뉴스 크롤러를 시작합니다 부르릉...")
            
            connection = create_connection()
            if not connection:
                logger.error("MySQL 연결 실패. 1분 후 재시도합니다.")
                time.sleep(60)
                continue
            
            news_urls = get_news_urls()
            
            for url in news_urls:
                try:
                    news_data = parse_news(url)
                    if news_data:
                        save_news_to_mysql(connection, news_data)
                    time.sleep(1)  # 개별 뉴스 크롤링 간 1초 대기
                        
                except Exception as e:
                    logger.error(f"뉴스 파싱 실패: {url}: {str(e)}")
                    continue
                    
            # 오래된 뉴스 정리
            clean_old_news(connection)
            
            logger.info("크롤링 작업이 완료되었습니다. 1시간 후에 다시 시작합니다.")
            time.sleep(3600)  # 1시간 대기
                
        except Exception as e:
            logger.error(f"크롤링 프로세스 오류: {str(e)}")
                
        finally:
            if 'connection' in locals() and connection:
                connection.close()

if __name__ == "__main__":
    main()