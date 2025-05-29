import logging
import re
import random
import time
import os
import tempfile
import uuid
from datetime import datetime
from typing import List, Dict, Tuple, Set, Optional

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

from ..models import JobListing, Skill
from ..database import insert_job_listing, insert_skill
from .base_scraper import BaseScraper


class PracujScraper(BaseScraper):
    """Scraper for pracuj.pl job board"""
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.pracuj.pl"
        self.search_url = (
            "https://it.pracuj.pl/praca/warszawa;wp?rd=30&et=3%2C17%2C4&its=big-data-science"
        )
        # skill_categories definition ... (unchanged)
        # [existing skill_categories dict here]

    def get_last_processed_page(self) -> int:
        checkpoint_path = os.path.join(tempfile.gettempdir(), "pracuj_checkpoint.txt")
        try:
            if os.path.exists(checkpoint_path):
                with open(checkpoint_path, "r") as f:
                    return int(f.read().strip())
        except Exception:
            logging.warning("Failed to read checkpoint, starting at 1")
        return 1

    def save_checkpoint(self, page_number: int):
        checkpoint_path = os.path.join(tempfile.gettempdir(), "pracuj_checkpoint.txt")
        try:
            with open(checkpoint_path, "w") as f:
                f.write(str(page_number))
            self.logger.info(f"Saved checkpoint for page {page_number}")
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")

    # [other helper methods unchanged: _extract_salary, _extract_badge_info, _extract_skills_from_listing, etc.]

    def scrape(self) -> Tuple[List[JobListing], Dict[str, List[str]]]:
        all_job_listings: List[JobListing] = []
        all_skills_dict: Dict[str, List[str]] = {}
        successful_db_inserts = 0

        # 1) Resume
        last_page = self.get_last_processed_page()
        current_page = last_page
        starting_page = current_page

        # 2) Detect total_pages
        first_html = self.get_page_html(self.search_url)
        first_soup = BeautifulSoup(first_html, "html.parser")
        total_pages: Optional[int] = None
        # a) "Strona X z Y"
        desc = first_soup.find(text=re.compile(r"Strona\s+\d+\s+z\s+\d+"))
        if desc:
            m = re.search(r"Strona\s+\d+\s+z\s+(\d+)", desc)
            if m:
                total_pages = int(m.group(1))
                self.logger.info(f"Detected total_pages={total_pages} from description")
        # b) buttons
        if total_pages is None:
            nums = [int(el.get_text(strip=True))
                    for el in first_soup.select("a, button")
                    if el.get_text(strip=True).isdigit()]
            if nums:
                total_pages = max(nums)
                self.logger.info(f"Detected total_pages={total_pages} from buttons")
        # c) '?pn=' params
        if total_pages is None:
            nums = [int(m.group(1))
                    for a in first_soup.find_all("a", href=True)
                    if (m := re.search(r"[?&]pn=(\d+)", a["href"]))]
            if nums:
                total_pages = max(nums)
                self.logger.info(f"Detected total_pages={total_pages} from URL params")
        # d) default
        if total_pages is None:
            total_pages = 1
            self.logger.warning("Could not detect pagination; defaulting to 1")

        # 3) Pagination bounds
        pages_per_run = total_pages
        end_page = min(starting_page + pages_per_run - 1, total_pages)
        self.logger.info(f"Scraping pages {starting_page}–{end_page} of {total_pages}")

        processed_urls: Set[str] = set()

        # 4) Loop
        while current_page <= end_page:
            self.logger.info(f"Processing page {current_page} of {end_page}")
            page_url = (
                self.search_url
                if current_page == 1
                else f"{self.search_url}&pn={current_page}"
            )
            self.logger.info(f"Fetching {page_url}")
            html = self.get_page_html(page_url)
            soup = BeautifulSoup(html, "html.parser")
            job_containers = soup.select("li.offer")

            # collect URLs
            job_urls: List[str] = []
            for c in job_containers:
                u = c.select_one("a.offer-link")["href"]
                if "pracodawcy.pracuj.pl/company" in u or u in processed_urls:
                    continue
                job_urls.append(u)
                processed_urls.add(u)

            # parallel fetch & parse
            listings: List[JobListing] = []
            with ThreadPoolExecutor(max_workers=8) as pool:
                futs = {pool.submit(self.get_page_html, u): u for u in job_urls}
                for fut in as_completed(futs):
                    u = futs[fut]
                    try:
                        detail_html = fut.result(timeout=60)
                        job = self._parse_job_detail(detail_html, u)
                        listings.append(job)
                    except Exception as e:
                        self.logger.error(f"Error parsing {u}: {e}")

            # insert jobs & skills
            for job in listings:
                job_id = insert_job_listing(job)
                if job_id:
                    successful_db_inserts += 1
                    skills = self._extract_skills_from_listing(BeautifulSoup(
                        self.get_page_html(job.link), "html.parser"))
                    all_skills_dict[job.job_id] = skills
                    for skill in skills:
                        insert_skill(job_id, skill)
                    all_job_listings.append(job)

            # checkpoint & next
            self.save_checkpoint(current_page + 1)
            current_page += 1
            time.sleep(random.uniform(2, 4))

        # summary
        self.logger.info(
            f"Processed {len(all_job_listings)} jobs over {current_page - starting_page} pages"
        )
        self.logger.info(f"Next run from page {current_page}")
        self.logger.info(f"Inserted {successful_db_inserts} new jobs")

        return all_job_listings, all_skills_dict

    def _parse_job_detail(self, html: str, job_url: str) -> JobListing:
        soup = BeautifulSoup(html, "html.parser")
        m = re.search(r",oferta,(\d+)", job_url)
        job_id = m.group(1) if m else job_url
        title = soup.select_one("h1.offer-title").get_text(strip=True)
        company = soup.select_one("a.company-name").get_text(strip=True)
        badges = self._extract_badge_info(soup)
        yoe = self._extract_years_of_experience(soup)
        return JobListing(
            job_id=job_id,
            source="pracuj.pl",
            title=title,
            company=company,
            link=job_url,
            salary_min=badges.get("salary_min"),
            salary_max=badges.get("salary_max"),
            location=badges.get("location", ""),
            operating_mode=badges.get("operating_mode", ""),
            work_type=badges.get("work_type", ""),
            experience_level=badges.get("experience_level", ""),
            employment_type=badges.get("employment_type", ""),
            years_of_experience=yoe,
            scrape_date=datetime.now(),
            listing_status="Active"
        )

def scrape_pracuj():
    scraper = PracujScraper()
    return scraper.scrape()
