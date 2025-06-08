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
        """Extract location, salary, experience, work type, employment type, operating mode."""
        result = {
            "location": "",
            "operating_mode": "",
            "work_type": "",
            "experience_level": "",
            "employment_type": "",
            "salary_min": None,
            "salary_max": None,
        }

        # ——— 1) Parse the <ul class="tiles_bfrsaoj"> block for experience/work/employment/remote ———
        badge_ul = soup.find("ul", class_=re.compile(r"tiles_bfrsaoj", re.I))
        if badge_ul:
            items = badge_ul.find_all("li", class_=re.compile(r"tiles_i14a41ct", re.I))
            for item in items:
                dt = item.get("data-test", "") or ""
                text = item.get_text(strip=True)

                # data-test="offer-additional-info-0" → Experience
                if dt.endswith("-0"):
                    result["experience_level"] = text
                # data-test="offer-additional-info-1" → Work type (e.g. Pełny etat)
                elif dt.endswith("-1"):
                    result["work_type"] = text
                # data-test="offer-additional-info-2" → Employment Type (e.g. Kontrakt B2B)
                elif dt.endswith("-2"):
                    result["employment_type"] = text
                # data-test="offer-additional-info-3" or "-4" → Operating mode
                elif dt.endswith("-3") or dt.endswith("-4"):
                    result["operating_mode"] = text

        # Fallback heuristics if any fields missing
        if not result["operating_mode"]:
            remote = soup.find("li", string=re.compile(r"zdaln|hybryd|stacjon", re.I))
            if remote:
                result["operating_mode"] = remote.get_text(strip=True)
        if not result["work_type"]:
            work_type = soup.find("li", string=re.compile(r"etat|kontrakt|umowa", re.I))
            if work_type:
                result["work_type"] = work_type.get_text(strip=True)

        # ——— 2) Fallbacks if any of those above were not in that UL ———
        # (you can keep your old fallback logic if you want, but usually the UL has everything)

        # ——— 3) Extract location (same as before) ———
        location_elem = soup.find('span', attrs={'data-test': 'offer-region'})
        if not location_elem:
            location_elem = soup.find('div', attrs={'data-test': 'offer-badge-title'})
        if not location_elem:
            location_elem = soup.find("span", string=re.compile(r"warszawa|kraków|wrocław|gdańsk|poznań", re.I))
        if location_elem:
            result['location'] = location_elem.get_text(strip=True)

        # ——— 4) Extract salary by looking for data-test="text-earningAmount" (Adjusted) ———
        salary_elem = soup.find('div', attrs={'data-test': 'text-earningAmount'})  # *Adjusted*
        if salary_elem:
            salary_text = salary_elem.get_text(" ", strip=True)
            min_sal, max_sal = self._extract_salary(salary_text)
            result['salary_min'] = min_sal
            result['salary_max'] = max_sal

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
        """Extract years of experience from requirements section"""
        # Try multiple selectors for requirements
        requirements_section = soup.find(
            "div",
            attrs={
                "data-test": "offer-sub-section",
                "data-scroll-id": "requirements-expected-1",
            },
        )

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
        """Extract a JobListing from a detail-page HTML."""
        soup = BeautifulSoup(html, "html.parser")

        # Extract stable ID from URL
        m = re.search(r",oferta,(\d+)", job_url)
        if not m:
            m = re.search(r"/oferta/[^/]+/(\d+)", job_url)
        job_id = m.group(1) if m else str(hash(job_url))[:8]

        # Extract title
        title_elem = (
            soup.select_one("h1[data-test='offer-title']")
            or soup.select_one("h1.offer-title")
            or soup.select_one("h1")
            or soup.select_one("a[data-test='link-offer-title']")
        )
        if not title_elem:
            meta_title = soup.find("meta", attrs={"property": "og:title"})
             if meta_title:
                title = meta_title["content"].strip()
            else:
                title_tag = soup.find("title")
                if title_tag:
                    title_text = title_tag.get_text(strip=True)
                    title = re.split(r"[|\-]", title_text)[0].strip()
                else:
                    title = "Unknown Title"
        else:
            title = title_elem.get_text(strip=True)

        # ——— Find company name (Adjusted: drop the broad /firma/ fallback) ———
        company_elem = (
            soup.select_one("a[data-test='offer-company-name']")
            or soup.select_one("a.company-name")
            or soup.select_one("h3[data-test='text-company-name']")
        )
        company = company_elem.get_text(strip=True) if company_elem else "Unknown Company"

        # Extract badges (location, salary, experience, etc.)
        badges = self._extract_badge_info(soup)

        # Extract years of experience
        yoe = self._extract_years_of_experience(soup)

        return JobListing(
            job_id=job_id,
            source="pracuj.pl",
            title=title,
            company=company,
            link=job_url,
            salary_min=badges["salary_min"],
            salary_max=badges["salary_max"],
            location=badges["location"],
            operating_mode=badges["operating_mode"],
            work_type=badges["work_type"],
            experience_level=badges["experience_level"],
            employment_type=badges["employment_type"],
            years_of_experience=yoe,
            scrape_date=datetime.now(),
            listing_status="Active",
        )

    def scrape(self) -> Tuple[List["JobListing"], Dict[str, List[str]]]:
        all_job_listings: List[JobListing] = []
        all_skills_dict: Dict[str, List[str]] = {}
        successful_db_inserts = 0

        # Get the last saved page, or start at 1
        current_page = self.get_last_processed_page()
        processed_urls: Set[str] = set()

        while True:
            logging.info(f"Processing page {current_page}")

            page_url = (
                self.search_url
                if current_page == 1
                else f"{self.search_url}&pn={current_page}"
            )
            html = self.get_page_html(page_url)
            soup = BeautifulSoup(html, "html.parser")

            # Collect all job-offer URLs on this page
            urls = []

            link_selector = "a[data-test='link-offer-title'][href], a.tiles_o1859gd9[href]"
            for link in soup.select(link_selector):
                href = link["href"]
                if "oferta" not in href:
                    continue

                # Skip employer profile or promo links
                if "pracodawcy.pracuj.pl/company" in href:
                    continue

                if href.startswith("/"):
                    href = self.base_url + href

                if href not in processed_urls:
                    urls.append(href)
                    processed_urls.add(href)

            logging.info(f"Found {len(urls)} job URLs on page {current_page}")

            # If this page has zero new URLs, we assume we've reached the end:
            if not urls:
                logging.info(f"No more jobs on page {current_page}. Stopping pagination.")
                break

            # Process each detail page in parallel
            page_listings = []
            with ThreadPoolExecutor(max_workers=8) as pool:
                fut2url = {pool.submit(self.get_page_html, u): u for u in urls}
                for fut in as_completed(fut2url):
                    u = fut2url[fut]
                    try:
                        detail_html = fut.result(timeout=60)
                        job_listing = self._parse_job_detail(detail_html, u)
                        page_listings.append(job_listing)

                        detail_soup = BeautifulSoup(detail_html, "html.parser")
                        skills = self._extract_skills_from_listing(detail_soup)
                        all_skills_dict[job_listing.job_id] = skills
                    except Exception as ex:
                        logging.error(f"Error parsing {u}: {ex}")

            # Insert job listings
            for job in page_listings:
                if insert_job_listing(job):
                    successful_db_inserts += 1

            all_job_listings.extend(page_listings)
            logging.info(f"Processed {len(page_listings)} jobs on page {current_page}")

            # Save a checkpoint so we can resume if it crashes
            self.save_checkpoint(current_page + 1)

            # Throttle a bit before next page
            time.sleep(random.uniform(2, 4))
            current_page += 1

        logging.info(
            f"Scrape complete: {len(all_job_listings)} jobs processed, "
            f"{successful_db_inserts} new inserts."
        )
        return all_job_listings, all_skills_dict


def scrape_pracuj():
    """Function to run the pracuj.pl scraper"""
    scraper = PracujScraper()
    return scraper.scrape()
