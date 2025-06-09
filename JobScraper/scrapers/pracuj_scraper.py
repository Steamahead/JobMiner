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

    def get_last_processed_page(self):
        """Retrieve the last processed page from a local file"""
        try:
            checkpoint_path = os.path.join(tempfile.gettempdir(), "pracuj_checkpoint.txt")
            if os.path.exists(checkpoint_path):
                with open(checkpoint_path, "r") as f:
                    checkpoint_data = f.read().strip()
                    return int(checkpoint_data)
            return 1
        except Exception as e:
            logging.warning(f"Failed to retrieve checkpoint: {str(e)}")
            return 1

    def save_checkpoint(self, page_number):
        """Save the current page as a checkpoint to a local file"""
        try:
            checkpoint_path = os.path.join(tempfile.gettempdir(), "pracuj_checkpoint.txt")
            with open(checkpoint_path, "w") as f:
                f.write(str(page_number))
            logging.info(f"Saved checkpoint for page {page_number}")
        except Exception as e:
            logging.error(f"Failed to save checkpoint: {str(e)}")

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

    def _extract_badge_info(self, soup: BeautifulSoup) -> Dict[str, object]:
            """
            Pull Location / OperatingMode / WorkType / ExperienceLevel / EmploymentType
            and SalaryMin / SalaryMax out of the in-offer benefit list and salary section.
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
    
            # Base selector for each badge in the <ul data-test="sections-benefit-list">
            base = 'ul[data-test="sections-benefit-list"] li[data-test="{dt}"] div[data-test="offer-badge-title"]'
    
            # — Location: office first, else remote —
            office = soup.select_one(base.format(dt="sections-benefit-workplaces"))
            remote = soup.select_one(base.format(dt="sections-benefit-workplaces-wp"))
            if office:
                result["Location"] = office.get_text(strip=True)
            elif remote:
                result["Location"] = remote.get_text(strip=True)
    
            # — EmploymentType (contract) —
            et = soup.select_one(base.format(dt="sections-benefit-contracts"))
            if et:
                result["EmploymentType"] = et.get_text(strip=True)
    
            # — WorkType (schedule) —
            wt = soup.select_one(base.format(dt="sections-benefit-work-schedule"))
            if wt:
                result["WorkType"] = wt.get_text(strip=True)
    
            # — ExperienceLevel (seniority) —
            el = soup.select_one(base.format(dt="sections-benefit-employment-type-name"))
            if el:
                result["ExperienceLevel"] = el.get_text(strip=True)
    
            # — OperatingMode (remote / hybrid / on-site) —
            om = soup.select_one(base.format(dt="sections-benefit-work-modes-many"))
            if om:
                result["OperatingMode"] = om.get_text(strip=True)
    
            # — Salary —
            # <div data-test="section-salary">…<div data-test="text-earningAmount">120,00 – 135,00 zł</div>…</div>
            sal = soup.select_one('div[data-test="section-salary"] div[data-test="text-earningAmount"]')
            if sal:
                # reuse your existing salary parser here
                min_sal, max_sal = self._extract_salary(sal.get_text(" ", strip=True))
                result["SalaryMin"], result["SalaryMax"] = min_sal, max_sal
    
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
        Finds the first <li class="tkzmjn3">…Min. X lat…</li> and returns X as an integer.
        """
        li = soup.select_one("li.tkzmjn3")
        if not li:
            return None
        text = li.get_text(" ", strip=True)
        m = re.search(r"Min\.\s*(\d+)", text)
        return int(m.group(1)) if m else None

        if not requirements_section:
            # Fallback: search in the entire page text
            page_text = soup.get_text().lower()
        else:
            page_text = requirements_section.get_text(strip=True).lower()

        patterns = [
            # Polish patterns
            r"(\d+)\s*rok\w*\s*doświadczeni",
            r"(\d+)\s*lat\w*\s*doświadczeni",
            r"doświadczeni\w*\s*(\d+)\s*rok",
            r"doświadczeni\w*\s*(\d+)\s*lat",
            r"min[.:]?\s*(\d+)\s*rok\w*\s*doświadczeni",
            r"min[.:]?\s*(\d+)\s*lat\w*\s*doświadczeni",
            # English patterns
            r"(\d+)\s*year\w*\s*experience",
            r"experience\s*of\s*(\d+)\s*year",
            r"min[.:]?\s*(\d+)\s*year\w*\s*experience",
        ]

        for pattern in patterns:
            match = re.search(pattern, page_text)
            if match:
                try:
                    return int(match.group(1))
                except (ValueError, IndexError):
                    pass

        return None

    def _parse_job_detail(self, html: str, job_url: str) -> JobListing:
        soup = BeautifulSoup(html, "html.parser")

        # — Job ID inline —
        m = re.search(r",oferta,(\d+)", job_url)
        if not m:
            m = re.search(r"/oferta/[^/]+/(\d+)", job_url)
        job_id = m.group(1) if m else str(hash(job_url))[:8]

        # — Title —
        # <h1 data-test="text-positionName">Architekt (…) </h1>
        title_el = soup.select_one("h1[data-test='text-positionName']")
        title = title_el.get_text(strip=True) if title_el else "Unknown Title"

        # — Company —
        # <h2 data-test="text-employerName">BCF Software Sp. z o.o.</h2>
        comp_el = soup.select_one("h2[data-test='text-employerName']")
        company = comp_el.get_text(strip=True) if comp_el else "Unknown Company"

        # — Badges (Location, contract, schedule, seniority, mode) + Salary —
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

    def scrape(self) -> Tuple[List[JobListing], Dict[str,List[str]]]:
        all_jobs = []
        all_skills = {}
        processed = set()
        current_page = self.get_last_processed_page()
        badge_map = {
            0: "experience_level",
            1: "work_type",
            2: "employment_type",
            4: "operating_mode",
        }
    
        while True:
            page_url = self.search_url if current_page == 1 else f"{self.search_url}&pn={current_page}"
            html = self.get_page_html(page_url)
            soup = BeautifulSoup(html, "html.parser")
    
            offers_div = soup.select_one("div[data-test='section-offers']")
            cards = offers_div and offers_div.find_all("div", class_=lambda c:c and "tiles_c1dxwih" in c)
            if not cards:
                break
    
            jobs_this_page = []
            # build tasks: [(url, container_soup), …]
            tasks = []
            for card in cards:
                a = card.select_one("a[data-test='link-offer-title']")
                if not a: 
                    continue
                href = a["href"]
                if href.startswith("/"):
                    href = self.base_url + href
                if href in processed or "pracodawcy.pracuj.pl/company" in href:
                    continue
                processed.add(href)
    
                # --- scrape everything from the card ---
                # ID
                m = re.search(r",oferta,(\d+)", href)
                job_id = m.group(1) if m else str(hash(href))[:8]
    
                # Title
                title = a.get_text(strip=True)
    
                # Company
                comp = card.select_one("h3[data-test='text-company-name']")
                company = comp.get_text(strip=True) if comp else "Unknown Company"
    
                # Salary
                sal = card.select_one("span[data-test='offer-salary']")
                salary_min, salary_max = self._extract_salary(sal.get_text(" ",strip=True)) if sal else (None,None)
    
                # Location
                loc = card.select_one("h4[data-test='text-region']")
                location = loc.get_text(strip=True) if loc else ""
    
                # Badges
                badges = {}
                for idx, field in badge_map.items():
                    li = card.select_one(f"li[data-test='offer-additional-info-{idx}']")
                    badges[field] = li.get_text(strip=True) if li else ""
    
                # enqueue detail‐page fetch
                tasks.append({
                    "url": href,
                    "meta": {
                        "job_id": job_id,
                        "title": title,
                        "company": company,
                        "salary_min": salary_min,
                        "salary_max": salary_max,
                        "location": location,
                        **badges
                    }
                })
    
            # now go off and fetch details in parallel
            with ThreadPoolExecutor(max_workers=8) as pool:
                fut2task = {
                    pool.submit(self.get_page_html, t["url"]): t
                    for t in tasks
                }
                for fut in as_completed(fut2task):
                    task = fut2task[fut]
                    detail_html = fut.result(timeout=60)
                    detail_soup = BeautifulSoup(detail_html, "html.parser")
    
                    # pull years-of-exp and skills from the detail page
                    yoe = self._extract_years_of_experience(detail_soup)
                    skills = self._extract_skills_from_listing(detail_soup)
    
                    m = task["meta"]
                    job = JobListing(
                        job_id   = m["job_id"],
                        source   = "pracuj.pl",
                        title    = m["title"],
                        company  = m["company"],
                        link     = task["url"],
                        salary_min       = m["salary_min"],
                        salary_max       = m["salary_max"],
                        location         = m["location"],
                        operating_mode   = m["operating_mode"],
                        work_type        = m["work_type"],
                        experience_level = m["experience_level"],
                        employment_type  = m["employment_type"],
                        years_of_experience = yoe,
                        scrape_date=datetime.now(),
                        listing_status="Active",
                    )
    
                    jobs_this_page.append(job)
                    all_skills[job.job_id] = skills
    
            # insert into DB, checkpoint, etc…
            all_jobs.extend(jobs_this_page)
            self.save_checkpoint(current_page+1)
            current_page += 1
    
        return all_jobs, all_skills

def scrape_pracuj():
    """Function to run the pracuj.pl scraper"""
    scraper = PracujScraper()
    return scraper.scrape()
