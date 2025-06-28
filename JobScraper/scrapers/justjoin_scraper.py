import requests
import json
import re
import time
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper
from ..database import insert_job_listing, insert_skill
from ..models import JobListing, Skill

class JustJoinScraper(BaseScraper):
    """
    Scraper for JustJoin.it job offers (Warszawa, junior data).
    Reuses the same DB insertion routines as Pracuj scraper.
    """
    BASE_URL = "https://justjoin.it"
    LISTING_URL = ("https://justjoin.it/job-offers/warszawa/data"
                   "?experience-level=junior&orderBy=DESC&sortBy=published&from={offset}")

    def scrape(self):
        offset = 0
        while True:
            detail_urls = self.scrape_listing_page(offset)
            if not detail_urls:
                break
            for url in detail_urls:
                try:
                    self.scrape_job_detail(url)
                except Exception as e:
                    self.logger.error(f"Error parsing {url}: {e}")
                time.sleep(1)
            offset += PAGE_SIZE

    def scrape_listing_page(self, offset):
        url = self.LISTING_URL.format(offset=offset)
        self.logger.info(f"Fetching listing page: {url}")
        resp = requests.get(url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("a.offer-card")
        urls = []
        for card in cards:
            href = card.get('href')
            if href:
                full_url = self.BASE_URL + href
                urls.append(full_url)
        self.logger.info(f"Found {len(urls)} job links")
        return urls

    def scrape_job_detail(self, url):
        self.logger.info(f"Fetching job detail: {url}")
        resp = requests.get(url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Parse JSON-LD
        ld = soup.find('script', type='application/ld+json')
        data = json.loads(ld.string) if ld and ld.string else {}

        # Core fields
        title = data.get('title')
        company = data.get('hiringOrganization', {}).get('name')
        location = (data.get('jobLocation', {})
                    .get('address', {}).get('addressLocality'))
        published_date = data.get('datePosted')

        # Salary range fallback
        salary_min = salary_max = None
        salary_tag = soup.select_one('span.mui-mrzdjb')
        if salary_tag:
            parts = salary_tag.stripped_strings
            nums = [s.replace('\u00a0', '').replace('PLN/month', '')
                    for s in parts if re.search(r'\d', s)]
            if len(nums) >= 2:
                salary_min, salary_max = nums[0], nums[1]

        # Metadata fields
        meta_divs = soup.select('div.MuiBox-root.mui-1ihbss1')
        # order: OperatingMode, WorkType, ExperienceLevel, EmploymentType
        operating_mode = meta_divs[0].get_text(strip=True) if len(meta_divs) > 0 else None
        work_type = meta_divs[1].get_text(strip=True) if len(meta_divs) > 1 else None
        experience_level = meta_divs[2].get_text(strip=True) if len(meta_divs) > 2 else None
        employment_type = meta_divs[3].get_text(strip=True) if len(meta_divs) > 3 else None

        # Years of experience: find <strong>Oczekiwania:</strong> and parse first <li>
        years_of_experience = None
        for strong in soup.find_all('strong'):
            if 'Oczekiwania' in strong.get_text():
                ul = strong.find_next('ul')
                if ul:
                    li = ul.find('li')
                    if li:
                        match = re.search(r'(\d+)[-–]?(?:-year)?', li.get_text())
                        if match:
                            years_of_experience = int(match.group(1))
                break

        # Insert into DB
        insert_job_listing(
            source='justjoin',
            external_id=url.split('/')[-1],
            title=title,
            company=company,
            location=location,
            published_date=published_date,
            salary_min=salary_min,
            salary_max=salary_max,
            operating_mode=operating_mode,
            work_type=work_type,
            experience_level=experience_level,
            employment_type=employment_type,
            years_of_experience=years_of_experience
        )

if __name__ == '__main__':
    scraper = JustJoinScraper()
    scraper.scrape()
