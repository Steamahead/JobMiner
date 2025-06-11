from typing import List, Dict, Tuple, Set, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import re
import time
from ..models import JobListing, Skill
from ..database import insert_job_listing, insert_skill
from .base_scraper import BaseScraper
import random, os, tempfile, uuid
from datetime import datetime
from bs4 import BeautifulSoup

class PracujScraper(BaseScraper):
    """Scraper for pracuj.pl job board"""

    def __init__(self):
        super().__init__()
        self.base_url = "https://www.pracuj.pl"
        self.search_url = (
            "https://it.pracuj.pl/praca/warszawa;wp?rd=30&et=3%2C17%2C4&its=big-data-science"
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
        # Detect if this was "per hour" by looking for zł/h or zł/godz
        is_hourly = 'zł/h' in clean_text or 'zł/godz' in clean_text
        clean_text = re.sub(r'[^\d,\.\-–]', '', clean_text)

        # Look for patterns like "12000–20000" or "150,00-180,00"
        match = re.search(r'([\d\.,]+)[–\-]([\d\.,]+)', clean_text)
        if match:
            try:
                min_val = match.group(1).replace(',', '.')
                max_val = match.group(2).replace(',', '.')
                min_salary = float(min_val)
                max_salary = float(max_val)

                if is_hourly:
                    # Convert hourly to monthly by assuming 160h/month
                    min_salary *= 160
                    max_salary *= 160

                return int(min_salary), int(max_salary)
            except ValueError:
                pass

        # If only a single number
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

    def _extract_badge_info(self, soup: BeautifulSoup) -> Dict[str,object]:
        """
        Extract SalaryMin/SalaryMax, Location, OperatingMode,
        WorkType, ExperienceLevel, EmploymentType from the in-offer
        benefit list + salary section.
        """
        result = {
            "SalaryMin":       None,
            "SalaryMax":       None,
            "Location":        "",
            "OperatingMode":   "",
            "WorkType":        "",
            "ExperienceLevel": "",
            "EmploymentType":  "",
        }
    
        base = 'ul[data-test="sections-benefit-list"] li[data-test="{dt}"] div[data-test="offer-badge-title"]'
    
        # Location: office first, else remote
        off = soup.select_one(base.format(dt="sections-benefit-workplaces"))
        wp  = soup.select_one(base.format(dt="sections-benefit-workplaces-wp"))
        if off:
            result["Location"] = off.get_text(strip=True)
        elif wp:
            result["Location"] = wp.get_text(strip=True)
    
        # EmploymentType (contract)
        et = soup.select_one(base.format(dt="sections-benefit-contracts"))
        if et:
            result["EmploymentType"] = et.get_text(strip=True)
    
        # WorkType (schedule)
        ws = soup.select_one(base.format(dt="sections-benefit-work-schedule"))
        if ws:
            result["WorkType"] = ws.get_text(strip=True)
    
        # ExperienceLevel (seniority)
        ex = soup.select_one(base.format(dt="sections-benefit-employment-type-name"))
        if ex:
            result["ExperienceLevel"] = ex.get_text(strip=True)
    
        # OperatingMode (mode)
        om = soup.select_one(base.format(dt="sections-benefit-work-modes-many"))
        if om:
            result["OperatingMode"] = om.get_text(strip=True)
    
        # SalaryMin & SalaryMax
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

        # Check the dedicated skills section
        skills_section = soup.find("ul", attrs={"data-test": "aggregate-open-dictionary-model"})
        if skills_section:
            skill_items = skills_section.find_all("li", class_="catru5k")
            for item in skill_items:
                skill_text = item.get_text(strip=True).lower()
                found_skills.add(skill_text)

        # If few/no skills found, check the description bullets
        if len(found_skills) < 2:
            description_section = soup.find("ul", attrs={"data-test": "aggregate-bullet-model"})
            if description_section:
                bullet_items = description_section.find_all("li", class_="tkzmjn3")
                description_text = " ".join([item.get_text(strip=True) for item in bullet_items])
                desc_skills = self._extract_skills_from_text(description_text)
                found_skills.update(desc_skills)

        # Fallback: search the entire page content
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

        # Simplified skill variations for key skills
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
            # Direct match to a standard skill
            for category, skills in self.skill_categories.items():
                if raw_skill in skills:
                    mapped_skills.add(raw_skill)
                    break

            # Check for variations
            for standard_skill, variations in skill_variations.items():
                if raw_skill in variations:
                    mapped_skills.add(standard_skill)
                    break

        return mapped_skills

    def _extract_years_of_experience(self, soup: BeautifulSoup) -> Optional[int]:
        """
        Extract required years of experience by scanning all <li class="tkzmjn3"> items,
        then falling back to the whole page if none match in the list.
        """
        lis = soup.select("li.tkzmjn3") or []

        # 1) Scan each <li> for the most common patterns in order:
        for li in lis:
            text = li.get_text(" ", strip=True).lower()

            # a) Hyphen ranges: "1-3 years", "3-4 letnim doświadczeniem"
            m = re.search(r"(\d+)[-–]\s*(\d+)", text)
            if m:
                return int(m.group(1))

            # b) Plus-signs: "3+ years of experience"
            m = re.search(r"(\d+)\+", text)
            if m:
                return int(m.group(1))

            # c) Min / Minimum: "Min. 1 roku doświadczenia", "Minimum 1-3 years"
            m = re.search(r"min(?:\.|imum)?\s*(\d+)", text)
            if m:
                return int(m.group(1))

            # d) Polish "rok"/"lat": "5 lat doświadczenia", "1 roku doświadczenia"
            m = re.search(r"(\d+)\s*rok\w*", text)
            if m:
                return int(m.group(1))
            m = re.search(r"(\d+)\s*lat\w*", text)
            if m:
                return int(m.group(1))

            # e) Specific Polish phrasing: "letnim doświadczeniem"
            m = re.search(r"(\d+)[-–]\s*(\d+)\s*letnim\s*doświadczeniem", text)
            if m:
                return int(m.group(1))

        # 2) Fallback: scan the entire page text
        page_text = soup.get_text(" ", strip=True).lower()
        fallback_patterns = [
            r"(\d+)[-–]\s*(\d+)",
            r"(\d+)\+",
            r"min(?:\.|imum)?\s*(\d+)",
            r"(\d+)\s*rok\w*",
            r"(\d+)\s*lat\w*",
            r"(\d+)[-–]\s*(\d+)\s*letnim\s*doświadczeniem",
        ]
        for pat in fallback_patterns:
            m = re.search(pat, page_text)
            if m:
                return int(m.group(1))

        # 3) No match found
        return None
        
    def _parse_job_detail(self, html: str, job_url: str) -> JobListing:
        soup = BeautifulSoup(html, "html.parser")
    
        # — ID inline —
        m = re.search(r",oferta,(\d+)", job_url)
        job_id = m.group(1) if m else str(hash(job_url))[:8]
    
        # — Title —
        # <h1 data-test="text-positionName">…</h1>
        t = soup.select_one("h1[data-test='text-positionName']")
        title = t.get_text(strip=True) if t else "Unknown Title"
    
        # — Company —
        # <h2 data-test="text-employerName">…</h2>
        c = soup.select_one("h2[data-test='text-employerName']")
        company = c.get_text(strip=True) if c else "Unknown Company"
    
        # — Badges / Salary / Location —
        badges = self._extract_badge_info(soup)
    
        # — Years of Experience —
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
        all_jobs = []
        all_skills = {}
        processed = set()
        current_page = 1
    
        while True:
            # 1) Fetch search results page
            page_url = (
                self.search_url
                if current_page == 1
                else f"{self.search_url}&pn={current_page}"
            )
            html = self.get_page_html(page_url)
            soup = BeautifulSoup(html, "html.parser")
    
            # 2) Find all job cards, skipping the “Oferty z innych lokalizacji” banner
            offers_div = soup.select_one("div[data-test='section-offers']")
            if not offers_div:
                break
        
            cards = []
            # iterate direct children so we see banner in order
            for element in offers_div.find_all(recursive=False):
                # skip that banner ad
                if element.select_one("div[data-test='range-box-section-title']"):
                    continue
                # only keep genuine job cards with a title link
                link = element.select_one("a[data-test='link-offer-title']")
                if not link:
                    continue
                cards.append(element)
        
            if not cards:
                break
    
            jobs_this_page = []
    
            # 3) Build a simple task list of {url, job_id}
            tasks = []
            for card in cards:
                a = card.select_one("a[data-test='link-offer-title']")
                href = a["href"]
                if href.startswith("/"):
                    href = self.base_url + href
                # skip duplicates & company‐profile links
                if href in processed or "pracodawcy.pracuj.pl/company" in href:
                    continue
                processed.add(href)
    
                m = re.search(r",oferta,(\d+)", href)
                job_id = m.group(1) if m else str(hash(href))[:8]
                tasks.append({"url": href, "job_id": job_id})
    
            # 4) Fetch detail pages in parallel and parse
            with ThreadPoolExecutor(max_workers=8) as pool:
                fut2task = {
                    pool.submit(self.get_page_html, t["url"]): t
                    for t in tasks
                }
                for fut in as_completed(fut2task):
                    task = fut2task[fut]
                    detail_html = fut.result(timeout=60)
    
                    # Parse every field from the detail page
                    job = self._parse_job_detail(detail_html, task["url"])
    
                    # Extract skills
                    detail_soup = BeautifulSoup(detail_html, "html.parser")
                    skills = self._extract_skills_from_listing(detail_soup)
    
                    jobs_this_page.append(job)
                    all_skills[job.job_id] = skills
    
            # 5) Persist
            all_jobs.extend(jobs_this_page)
            current_page += 1
   
        return all_jobs, all_skills


def scrape_pracuj():
    """Function to run the pracuj.pl scraper"""
    scraper = PracujScraper()
    return scraper.scrape()
