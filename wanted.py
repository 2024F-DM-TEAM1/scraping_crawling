import requests
import pandas as pd
import time
from typing import List, Dict, Optional
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProgrammersCrawler:
    def __init__(self):
        self.base_url = 'https://career.programmers.co.kr/api/job_positions'
        self.detail_url = 'https://career.programmers.co.kr/api/job_positions'
        self.job_ids = set()
        self.results = []

    def safe_get(self, data: Dict, keys: List[str], default: any = None) -> any:
        """안전하게 중첩된 딕셔너리에서 값을 가져오는 헬퍼 함수"""
        try:
            result = data
            for key in keys:
                result = result[key]
            return result if result is not None else default
        except (KeyError, TypeError, AttributeError):
            return default

    def safe_split(self, text: Optional[str], delimiter: str = "\n") -> List[str]:
        """문자열을 안전하게 분할하는 헬퍼 함수"""
        if not text:
            return []
        return [item.strip() for item in text.replace("• ", "").replace("•", "").split(delimiter) if item.strip()]

    def get_job_list(self, page: int) -> List[str]:
        """채용공고 목록을 가져오는 함수"""
        url = f'{self.base_url}?min_career=0&order=recent&page={page}'

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # jobPosition 배열에서 id 추출
            job_ids = [str(item.get('id')) for item in data.get('jobPositions', []) if item.get('id')]

            if not job_ids:
                logger.warning(f"No data found for page {page}")
                return []

            return job_ids

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching job list: {e}")
            return []

    def get_job_detail(self, job_id: str) -> Optional[Dict]:
        """채용공고 상세 정보를 가져오는 함수"""
        url = f'{self.detail_url}/{job_id}'

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            job_position = data.get('jobPosition', {})

            # 카테고리 처리
            categories = [str(cat_id) for cat_id in job_position.get('jobCategoryIds', [])]

            # 기술 스택 처리
            tech_stacks = [tag.get('name', '') for tag in job_position.get('technicalTags', [])]

            # 설명 텍스트 분할
            description = job_position.get('description', '')
            description_lines = self.safe_split(description)

            job_detail = {
                'title': job_position.get('title', '제목 없음'),
                'intro': job_position.get('description', '소개 없음'),
                'category': categories or ['카테고리 없음'],
                'comp_name': self.safe_get(job_position, ['company', 'name'], '회사명 없음'),
                'comp_addr': job_position.get('address', '주소 없음'),
                'role': description_lines or ['역할 정보 없음'],
                'requirement': description_lines or ['자격요건 없음'],
                'preferred': self.safe_split(job_position.get('preferredExperience')) or ['우대사항 없음'],
                'due': job_position.get('endAt', '마감일 미정'),
                'tech_stack': tech_stacks or ['기술 스택 없음'],
                'welfare': self.safe_split(job_position.get('additionalInformation')) or ['복지 정보 없음'],
                'procedure': job_position.get('additionalInformation', '채용 절차 미정')
            }

            return job_detail

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching job detail for ID {job_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error processing job ID {job_id}: {e}")
            return None

    def crawl(self, max_pages: int = 10):
        """전체 크롤링 프로세스를 실행하는 함수"""
        logger.info("Starting crawling process...")

        for page in range(1, max_pages + 1):  # Programmers는 1부터 시작
            new_ids = self.get_job_list(page)

            if not new_ids:
                logger.info("No more jobs to collect")
                break

            self.job_ids.update(new_ids)
            logger.info(f"Collected {len(self.job_ids)} unique job IDs...")
            time.sleep(0.5)

        for job_id in self.job_ids:
            job_detail = self.get_job_detail(job_id)
            if job_detail:
                self.results.append(job_detail)
            time.sleep(0.5)

        logger.info(f"Completed collecting {len(self.results)} job details")

    def save_to_csv(self, filename: str = None):
        """수집한 데이터를 CSV 파일로 저장하는 함수"""
        if not filename:
            filename = f'programmers_jobs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

        if not self.results:
            logger.warning("No data to save")
            return

        try:
            df = pd.DataFrame(self.results)
            # 리스트 형태의 컬럼들을 문자열로 변환
            list_columns = ['category', 'role', 'requirement', 'preferred', 'tech_stack', 'welfare']
            for col in list_columns:
                df[col] = df[col].apply(lambda x: '|'.join(x) if isinstance(x, list) else str(x))

            df.to_csv(filename, index=False, encoding='utf-8-sig')
            logger.info(f"Data saved to {filename}")

        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")


def main():
    crawler = ProgrammersCrawler()
    crawler.crawl()
    crawler.save_to_csv()


if __name__ == "__main__":
    main()