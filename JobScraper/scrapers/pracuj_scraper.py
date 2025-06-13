import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Tuple, Set, Optional, Union

from .base_scraper import BaseScraper
from ..models import JobListing, Skill
from ..database import insert_job_listing, insert_skill


class PracujScraper(BaseScraper):
    """Scraper for pracuj.pl job board"""

    def __init__(self):
        super().__init__()
        self.base_url = "https://www.pracuj.pl"
        self.search_url = (
            "https://www.pracuj.pl/praca/warszawa;wp?rd=30&et=3%2C17%2C4&its=big-data-science"
        )
        # Define skill categories
        self.skill_categories = {
            "Database": [
                "sql", "mysql", "postgresql", "oracle", "nosql", "mongodb", "database",
                "ms access", "sqlite", "redshift", "snowflake", "microsoft sql server",
                "teradata", "clickhouse",
            ],
            "Microsoft BI & Excel": [
                "excel", "power query", "power pivot", "vba", "macros", "pivot tables",
                "excel formulas", "spreadsheets", "m code", "ssrs", "ssis", "ssas",
                "power apps", "power automate", "powerpoint", "office 365",
            ],
            "Visualization": [
                "power bi", "tableau", "qlik", "looker", "data studio", "powerbi", "dax",
                "matplotlib", "seaborn", "plotly", "excel charts", "dashboard", "reporting",
                "d3.js", "grafana", "kibana", "google charts", "quicksight",
            ],
            "Programming": [
                "python", "r", "java", "scala", "c#", ".net", "javascript", "typescript",
                "vba", "pandas", "numpy", "jupyter", "scikit-learn", "tidyverse", "julia",
                "sql scripting", "pl/sql", "t-sql",
            ],
            "Data Processing": [
                "etl", "spark", "hadoop", "kafka", "airflow", "data engineering", "big data",
                "data cleansing", "data transformation", "data modeling", "data warehouse",
                "databricks", "dbt", "talend", "informatica",
            ],
            "Analytics & Statistics": [
                "statistics", "regression", "forecasting", "analytics", "analysis", "spss",
                "sas", "stata", "hypothesis testing", "a/b testing", "statistical",
                "time series", "clustering", "segmentation", "correlation",
            ],
            "Cloud": [
                "aws", "azure", "gcp", "google cloud", "cloud", "onedrive", "sharepoint",
                "snowflake", "databricks", "lambda", "s3",
            ],
            "Business Intelligence": [
                "business intelligence", "bi", "cognos", "business objects", "microstrategy",
                "olap", "data mart", "reporting", "kpi", "metrics", "domo", "sisense",
            ],
            "Machine Learning and AI": [
                "machine learning", "scikit-learn", "tensorflow", "keras", "pytorch", "deep learning",
                "xgboost", "lightgbm", "nlp", "computer vision", "anomaly detection", "feature engineering",
            ],
            "Data Governance and Quality": [
                "data governance", "data quality", "data integrity", "data validation",
                "master data management", "metadata", "data lineage", "data catalog",
            ],
            "Data Privacy and Security": [
                "data privacy", "gdpr", "data security", "compliance", "pii", "data anonymization",
            ],
            "Project Management and Soft Skills": [
                "project management", "agile", "scrum", "communication", "presentation", "storytelling",
                "collaboration", "stakeholder management", "requirements gathering", "jira", "confluence",
            ],
            "Version Control": [
                "git", "github", "gitlab", "version control", "bitbucket",
            ],
            "Data Integration and APIs": [
                "api", "rest api", "data integration", "web scraping", "etl tools", "soap", "ip rotation services",
            ],
            "ERP and CRM Systems": [
                "sap", "oracle", "salesforce", "dynamics", "erp", "crm", "workday",
            ],
        }

    def _extract_salary(self, salary_text: str) -> Tuple[Optional[int], Optional[int]]:
        """Extract min and max salary from salary text, handling hourly rates"""
        if not salary_text:
            return None, None

        clean_text = salary_text.replace('\xa0', '').replace('&nbsp;', '').replace(' ', '')
        is_hourly = 'zł/h' in clean_text or 'zł/godz' in clean_text
        clean_text = re.sub(r'[^\d,\.\-–]', '', clean_text)

        match = re.search(r'([\d\.,]+)[–\-]([\d\.,]+)', clean_text)
        if match:
            try:
                min_val = match.group(1).replace(',', '.')
                max_val = match.group(2).replace(',', '.')
                min_salary = float(min_val)
                max_salary = float(max_val)
                if is_hourly:
                    min_salary *= 160
                    max_salary *= 160
                return int(min_salary), int(max_salary)
            except ValueError:
                pass

        match = re.search(r'([\d\.,]+)', clean_text)
        if match:
            try:
                val = match.group(1).replace(',', '.')
                salary = float(val)
                if is_hourly:
                    salary *= 160
                return int(salary), int(salary)
            except ValueError:
                pass

        return None, None

    def _extract_badge_info(self, soup: BeautifulSoup) -> Dict[str, object]:
        """
        Extract SalaryMin/SalaryMax, Location, OperatingMode,
        WorkType, ExperienceLevel, EmploymentType from the in-offer
        benefit list + salary section.
        """
        result = {
            "SalaryMin": None,
            "SalaryMax": None,
            "Location": "",
            "OperatingMode": "",
            "WorkType": "",
            "ExperienceLevel": "",
            "EmploymentType": "",
        }

        base = 'ul[data-test="sections-benefit-list"] li[data-test="{dt}"] div[data-test="offer-badge-title"]'

        off = soup.select_one(base.format(dt="sections-benefit-workplaces"))
        wp = soup.select_one(base.format(dt="sections-benefit-workplaces-wp"))
        if off:
            result["Location"] = off.get_text(strip=True)
        elif wp:
            result["Location"] = wp.get_text(strip=True)

        et = soup.select_one(base.format(dt="sections-benefit-contracts"))
        if et:
            result["EmploymentType"] = et.get_text(strip=True)

        ws = soup.select_one(base.format(dt="sections-benefit-work-schedule"))
        if ws:
            result["WorkType"] = ws.get_text(strip=True)

        ex = soup.select_one(base.format(dt="sections-benefit-employment-type-name"))
        if ex:
            result["ExperienceLevel"] = ex.get_text(strip=True)

        om = soup.select_one(base.format(dt="sections-benefit-work-modes-many"))
        if om:
            result["OperatingMode"] = om.get_text(strip=True)

        if not result["OperatingMode"]:
            all_badges = soup.select('div[data-test="offer-badge-title"]')
            for div in all_badges:
                text = div.get_text(strip=True)
                if text not in result.values():
                    result["OperatingMode"] = text
                    break

        sal = soup.select_one(
            'div[data-test="section-salary"] div[data-test="text-earningAmount"]'
        )
        if sal:
            mn, mx = self._extract_salary(sal.get_text(" ", strip=True))
            result["SalaryMin"], result["SalaryMax"] = mn, mx

        return result

    def _extract_skills_from_listing(self, soup: BeautifulSoup) -> List[str]:
        """Extract skills from the dedicated skills section and/or description"""
        found_skills = set()

        skills_section = soup.find("ul", attrs={"data-test": "aggregate-open-dictionary-model"})
        if skills_section:
            skill_items = skills_section.find_all("li", class_="catru5k")
            for item in skill_items:
                skill_text = item.get_text(strip=True).lower()
                found_skills.add(skill_text)

        if len(found_skills) < 2:
            description_section = soup.find("ul", attrs={"data-test": "aggregate-bullet-model"})
            if description_section:
                bullet_items = description_section.find_all("li", class_="tkzmjn3")
                description_text = " ".join([item.get_text(strip=True) for item in bullet_items])
                desc_skills = self._extract_skills_from_text(description_text)
                found_skills.update(desc_skills)

        if len(found_skills) < 2:
            page_text = soup.get_text()
            desc_skills = self._extract_skills_from_text(page_text)
            found_skills.update(desc_skills)

        mapped_skills = self._map_to_standard_skills(found_skills)
        return list(mapped_skills)

    def _extract_skills_from_text(self, text: str) -> Set[str]:
        """Extract skills from description text"""
        all_skills = []
        for category, skills in self.skill_categories.items():
            all_skills.extend(skills)

        found_skills = set()
        text_lower = text.lower()

        for skill in all_skills:
            pattern = r'\b{}\b'.format(re.escape(skill))
            if re.search(pattern, text_lower):
                found_skills.add(skill)

        return found_skills

    def _map_to_standard_skills(self, raw_skills: Set[str]) -> Set[str]:
        """Map raw skill texts to our standardized skill names"""
        mapped_skills = set()

        skill_variations = {
            "sql": ["sql", "structured query language", "sql server", "t-sql"],
            "python": ["python", "język python"],
            "power bi": ["power bi", "powerbi", "power-bi"],
            "excel": ["excel", "microsoft excel", "ms excel"],
            "tableau": ["tableau"],
            "java": ["java"],
            "javascript": ["javascript", "js"],
            "azure": ["azure", "microsoft azure"],
            "aws": ["aws", "amazon web services"],
        }

        for raw_skill in raw_skills:
            for category, skills in self.skill_categories.items():
                if raw_skill in skills:
                    mapped_skills.add(raw_skill)
                    break

            for standard_skill, variations in skill_variations.items():
                if raw_skill in variations:
                    mapped_skills.add(standard_skill)
                    break

        return mapped_skills

    def _extract_years_of_experience(
        self, soup: BeautifulSoup
    ) -> Optional[Union[int, str]]:
        """
        1) Look only in the requirements bullets for any phrasing of years:
        …
        """
        bullets = soup.select(
            "ul[data-test='aggregate-bullet-model'] li.tkzmjn3"
        ) or []

        for li in bullets:
            text = li.get_text(" ", strip=True).lower()
            # a) "kilkuletn" / "wieloletn"
            if "kilkuletn" in text or "wieloletn" in text:
                return "several years"
            # b) hyphen ranges
            if m := re.search(r"\b([1-9]|1[0-2])[-–]\s*(?:[1-9]|1[0-2])", text):
                return int(m.group(1))
            # c) plus-sign
            if m := re.search(r"\b([1-9]|1[0-2])\+\s*years?\b", text):
                return int(m.group(1))
            # d) Min./Minimum
            if m := re.search(
                r"\bmin(?:\.|imum)?\s*([1-9]|1[0-2])\b.*(?:rok|lat|years?)", text
            ):
                return int(m.group(1))
            # e) integer + keyword
            if m := re.search(
                r"\b([1-9]|1[0-2])\s*(?:lat\w*|rok\w*|years?)\b", text
            ):
                return int(m.group(1))

        return None

    def _parse_job_detail(self, html: str, job_url: str) -> JobListing:
        soup = BeautifulSoup(html, "html.parser")

        # Job ID
        m = re.search(r",oferta,(\d+)", job_url)
        job_id = m.group(1) if m else str(hash(job_url))[:8]

        # Title
        t = soup.select_one("h1[data-test='text-positionName']")
        title = t.get_text(strip=True) if t else "Unknown Title"

        # Company
        c = soup.select_one("h2[data-test='text-employerName']")
        if not c:
            self.logger.warning(f"No <h2 data-test='text-employerName'> for {job_url}")
        company = c.find(text=True, recursive=False).strip() if c else "Unknown Company"

        # Badges / Salary / Location
        badges = self._extract_badge_info(soup)

        # Years of Experience
        yoe = self._extract_years_of_experience(soup)

        return JobListing(
            job_id=job_id,
            source="pracuj.pl",
            title=title,
            company=company,
            link=job_url,
            salary_min=badges["SalaryMin"],
            salary_max=badges["SalaryMax"],
            location=badges["Location"],
            operating_mode=badges["OperatingMode"],
            work_type=badges["WorkType"],
            experience_level=badges["ExperienceLevel"],
            employment_type=badges["EmploymentType"],
            years_of_experience=yoe,
            scrape_date=datetime.now(),
            listing_status="Active",
        )

    def scrape(self) -> Tuple[List[JobListing], Dict[str, List[str]]]:
        all_jobs: List[JobListing] = []
        all_skills: Dict[str, List[str]] = {}
        processed: Set[str] = set()
        current_page = 1

        while True:
            page_url = (
                self.search_url
                if current_page == 1
                else f"{self.search_url}&pn={current_page}"
            )
            html = self.get_page_html(page_url)
            soup = BeautifulSoup(html, "html.parser")

            offer_links = soup.select(
                "div[data-test='section-offers'] a[data-test='link-offer-title']"
            )
            if not offer_links:
                break

            tasks = []
            for a in offer_links:
                href = a["href"]
                if href.startswith("/"):
                    href = urljoin(self.search_url, href)
                if href in processed or "pracodawcy.pracuj.pl/company" in href:
                    continue
                processed.add(href)
                m = re.search(r",oferta,(\d+)", href)
                job_id = m.group(1) if m else str(hash(href))[:8]
                tasks.append({"url": href, "job_id": job_id})

            # **Fix:** assign fut2task so we can map back
            fut2task = {
                pool.submit(self.get_page_html, t["url"]): t
                for t in tasks
            }
            with ThreadPoolExecutor(max_workers=8) as pool:
                for fut in as_completed(fut2task):
                    task = fut2task[fut]
                    try:
                        detail_html = fut.result(timeout=60)
                    except Exception as e:
                        self.logger.warning(f"Timeout or error fetching {task['url']}: {e}")
                        continue

                    if not detail_html or "<h1" not in detail_html:
                        self.logger.warning(f"No content for {task['url']} – skipping")
                        continue

                    job = self._parse_job_detail(detail_html, task["url"])
                    detail_soup = BeautifulSoup(detail_html, "html.parser")
                    skills = self._extract_skills_from_listing(detail_soup)

                    all_jobs.append(job)
                    all_skills[job.job_id] = skills

            current_page += 1

        return all_jobs, all_skills


def scrape_pracuj():
    """Function to run the pracuj.pl scraper"""
    scraper = PracujScraper()
    return scraper.scrape()
