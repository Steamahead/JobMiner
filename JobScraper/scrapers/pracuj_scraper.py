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
        self.search_url = "https://it.pracuj.pl/praca/warszawa;wp?rd=30&et=3%2C17%2C4&its=big-data-science"
        # Define skill categories
        self.skill_categories = {
            "Database": [
                "sql", "mysql", "postgresql", "oracle", "nosql", "mongodb", "database", "ms access", 
                "sqlite", "redshift", "snowflake", "microsoft sql server", "teradata", "clickhouse"
            ],

            "Microsoft BI & Excel": [
                "excel", "power query", "power pivot", "vba", "macros", "pivot tables", 
                "excel formulas", "spreadsheets", "m code", "ssrs", "ssis", "ssas", 
                "power apps", "power automate", "powerpoint", "office 365"
            ],

            "Visualization": [
                "power bi", "tableau", "qlik", "looker", "data studio", "powerbi", "dax", 
                "matplotlib", "seaborn", "plotly", "excel charts", "dashboard", "reporting", "d3.js",
                "grafana", "kibana", "google charts", "quicksight"
            ],

            "Programming": [
                "python", "r", "java", "scala", "c#", ".net", "javascript", "typescript", 
                "vba", "pandas", "numpy", "jupyter", "scikit-learn", "tidyverse", "julia",
                "sql scripting", "pl/sql", "t-sql"
            ],

            "Data Processing": [
                "etl", "spark", "hadoop", "kafka", "airflow", "data engineering", "big data",
                "data cleansing", "data transformation", "data modeling", "data warehouse",
                "databricks", "dbt", "talend", "informatica"
            ],

            "Analytics & Statistics": [
                "statistics", "regression", "forecasting", "analytics", "analysis", "spss", 
                "sas", "stata", "hypothesis testing", "a/b testing", "statistical", 
                "time series", "clustering", "segmentation", "correlation"
            ],

            "Cloud": [
                "aws", "azure", "gcp", "google cloud", "cloud", "onedrive", "sharepoint",
                "snowflake", "databricks", "lambda", "s3"
            ],

            "Business Intelligence": [
                "business intelligence", "bi", "cognos", "business objects", "microstrategy",
                "olap", "data mart", "reporting", "kpi", "metrics", "domo", "sisense"
            ],

            "Machine Learning and AI": [
                "machine learning", "scikit-learn", "tensorflow", "keras", "pytorch", "deep learning",
                "xgboost", "lightgbm", "nlp", "computer vision", "anomaly detection", "feature engineering"
            ],

            "Data Governance and Quality": [
                "data governance", "data quality", "data integrity", "data validation",
                "master data management", "metadata", "data lineage", "data catalog"
            ],

            "Data Privacy and Security": [
                "data privacy", "gdpr", "data security", "compliance", "pii", "data anonymization"
            ],

            "Project Management and Soft Skills": [
                "project management", "agile", "scrum", "communication", "presentation", "storytelling", 
                "collaboration", "stakeholder management", "requirements gathering", "jira", "confluence",
                "atlassian"
            ],

            "Version Control": [
                "git", "github", "gitlab", "version control", "bitbucket"
            ],

            "Data Integration and APIs": [
                "api", "rest api", "data integration", "web scraping", "etl tools", "soap",
                "ip rotation services"
            ],

            "ERP and CRM Systems": [
                "sap", "oracle", "salesforce", "dynamics", "erp", "crm", "workday"
            ]
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
                    min_salary = min_salary * 160
                    max_salary = max_salary * 160
                
                return int(min_salary), int(max_salary)
            except ValueError:
                pass

        # Look for single value
        match = re.search(r'([\d\.,]+)', clean_text)
        if match:
            try:
                val = match.group(1).replace(',', '.')
                salary = float(val)
                
                if is_hourly:
                    salary = salary * 160
                
                return int(salary), int(salary)
            except ValueError:
                pass

        return None, None

    def _extract_badge_info(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract information from badge elements using updated selectors"""
        result = {
            'location': '',
            'operating_mode': '',
            'work_type': '',
            'experience_level': '',
            'employment_type': '',
            'salary_min': None,
            'salary_max': None
        }

        # Get operating mode (work modes)
        operating_mode_elem = soup.find("li", attrs={"data-scroll-id": "work-modes"})
        if not operating_mode_elem:
            operating_mode_elem = soup.find("span", string=re.compile(r"(praca zdalna|hybrydow|stacjonar)", re.I))
        if operating_mode_elem:
            result['operating_mode'] = operating_mode_elem.get_text(strip=True)

        # Get work type (work schedules)
        work_type_elem = soup.find("li", attrs={"data-test": "sections-benefit-work-schedule"})
        if not work_type_elem:
            work_type_elem = soup.find("span", string=re.compile(r"(pełny etat|część etatu|umowa)", re.I))
        if work_type_elem:
            result['work_type'] = work_type_elem.get_text(strip=True)

        # Get location - try multiple selectors
        location_elem = soup.find('span', attrs={'data-test': 'offer-region'})
        if not location_elem:
            location_elem = soup.find('div', attrs={'data-test': 'offer-badge-title'})
        if not location_elem:
            location_elem = soup.find("span", string=re.compile(r"warszawa|kraków|wrocław|gdańsk|poznań", re.I))
        
        if location_elem:
            result['location'] = location_elem.get_text(strip=True)

        # Try to extract salary
        salary_elem = soup.find('span', attrs={'data-test': 'offer-salary'})
        if salary_elem:
            salary_text = salary_elem.get_text(strip=True)
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
        requirements_section = soup.find("div", attrs={"data-test": "offer-sub-section", 
                                                      "data-scroll-id": "requirements-expected-1"})
        
        if not requirements_section:
            # Fallback: search in the entire page text
            page_text = soup.get_text().lower()
        else:
            page_text = requirements_section.get_text(strip=True).lower()

        patterns = [
            # Polish patterns
            r'(\d+)\s*rok\w*\s*doświadczeni',
            r'(\d+)\s*lat\w*\s*doświadczeni',
            r'doświadczeni\w*\s*(\d+)\s*rok',
            r'doświadczeni\w*\s*(\d+)\s*lat',
            r'min[.:]?\s*(\d+)\s*rok\w*\s*doświadczeni',
            r'min[.:]?\s*(\d+)\s*lat\w*\s*doświadczeni',
            # English patterns
            r'(\d+)\s*year\w*\s*experience',
            r'experience\s*of\s*(\d+)\s*year',
            r'min[.:]?\s*(\d+)\s*year\w*\s*experience',
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
        m = re.search(r',oferta,(\d+)', job_url)
        if not m:
            m = re.search(r'/oferta/[^/]+/(\d+)', job_url)
        job_id = m.group(1) if m else str(hash(job_url))[:8]

        # Extract title + company with multiple fallbacks
        title_elem = (soup.select_one("h1[data-test='offer-title']") or 
                     soup.select_one("h1.offer-title") or 
                     soup.select_one("h1"))
        title = title_elem.get_text(strip=True) if title_elem else "Unknown Title"
        
        company_elem = (soup.select_one("a[data-test='offer-company-name']") or
                       soup.select_one("a.company-name") or
                       soup.find("a", href=re.compile(r"/firma/")))
        company = company_elem.get_text(strip=True) if company_elem else "Unknown Company"

        # Extract badges (location, salary, etc.)
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
            listing_status="Active"
        )

def scrape(self) -> Tuple[List, Dict]:
    """Main scraping method"""
    all_job_listings: List[JobListing] = []
    all_skills_dict: Dict[str, List[str]] = {}
    successful_db_inserts = 0

    last_page = self.get_last_processed_page()
    current_page = last_page
    starting_page = last_page

    # Detect total pages - improved logic
    first_html = self.get_page_html(self.search_url)
    first_soup = BeautifulSoup(first_html, "html.parser")
    total_pages = 1

    # Debug: Log the page structure
    logging.info(f"Page title: {first_soup.title.string if first_soup.title else 'No title'}")
    
    # Method 1: Look for pagination text like "Strona 1 z 5"
    pagination_text = first_soup.find(string=re.compile(r"Strona\s+\d+\s+z\s+\d+", re.I))
    if pagination_text:
        match = re.search(r"Strona\s+\d+\s+z\s+(\d+)", pagination_text, re.I)
        if match:
            total_pages = int(match.group(1))
            logging.info(f"Method 1: Detected total_pages={total_pages} from pagination text")
    
    # Method 2: Look for pagination navigation buttons/links
    if total_pages == 1:
        # Try to find pagination container
        pagination_container = (first_soup.find('nav', {'aria-label': re.compile(r'paginat', re.I)}) or
                              first_soup.find('div', class_=lambda x: x and 'paginat' in x.lower()) or
                              first_soup.find('ul', class_=lambda x: x and 'paginat' in x.lower()))
        
        if pagination_container:
            # Look for all page number links
            page_links = pagination_container.find_all('a', href=re.compile(r'[?&]pn=\d+'))
            page_numbers = []
            for link in page_links:
                match = re.search(r'[?&]pn=(\d+)', link['href'])
                if match:
                    page_numbers.append(int(match.group(1)))
            
            if page_numbers:
                total_pages = max(page_numbers)
                logging.info(f"Method 2: Detected total_pages={total_pages} from pagination links")
    
    # Method 3: Look for "Next" button and estimate pages (fallback)
    if total_pages == 1:
        next_button = first_soup.find('a', string=re.compile(r'(następna|next)', re.I))
        if next_button:
            # If there's a next button, assume at least 2 pages, but let's be conservative and scan
            total_pages = 10  # Set a reasonable maximum to scan
            logging.info(f"Method 3: Found 'Next' button, will scan up to {total_pages} pages")
    
    logging.info(f"Final detected total_pages: {total_pages}")

    # Reset if checkpoint beyond total pages
    if starting_page > total_pages:
        logging.info("Checkpoint > total_pages: resetting to page 1")
        current_page = starting_page = 1

    end_page = min(starting_page + total_pages - 1, total_pages)
    logging.info(f"Scraping pages {starting_page}–{end_page} of {total_pages}")

    processed_urls: Set[str] = set()

    # Pagination loop
    while current_page <= end_page:
        try:
            logging.info(f"Processing page {current_page}/{end_page}")
            page_url = (
                self.search_url
                if current_page == 1
                else f"{self.search_url}&pn={current_page}"
            )
            html = self.get_page_html(page_url)
            soup = BeautifulSoup(html, "html.parser")

            # Try multiple selectors to find job offers
            urls = []
            
            # Method 1: Look for standard job offer selectors
            li_offers = soup.select("li.offer")
            logging.info(f"Found {len(li_offers)} li.offer elements")
            
            if not li_offers:
                # Method 2: Try different selectors
                li_offers = soup.select("div[data-test='default-offer']")
                logging.info(f"Found {len(li_offers)} div[data-test='default-offer'] elements")
            
            if not li_offers:
                # Method 3: Look for any links that contain 'oferta'
                all_links = soup.find_all('a', href=True)
                offer_links = [link for link in all_links if 'oferta' in link['href']]
                logging.info(f"Found {len(offer_links)} links containing 'oferta'")
                
                for link in offer_links[:20]:  # Limit to first 20 to avoid spam
                    href = link['href']
                    if href.startswith('/'):
                        href = self.base_url + href
                    if href not in processed_urls:
                        urls.append(href)
                        processed_urls.add(href)
            else:
                # Process standard job offers
                for li in li_offers:
                    href_elem = (li.select_one("a[data-test='link-offer']") or
                               li.select_one("a.offer-link") or
                               li.select_one("a[href*='oferta']"))
                    
                    if href_elem and href_elem.get("href"):
                        href = href_elem["href"]
                        if href.startswith('/'):
                            href = self.base_url + href
                        
                        if "pracodawcy.pracuj.pl/company" in href or href in processed_urls:
                            continue
                        urls.append(href)
                        processed_urls.add(href)

            logging.info(f"Found {len(urls)} job URLs to process")

            # If no URLs found, log some debug info
            if not urls:
                logging.warning("No job URLs found! Debugging page structure:")
                # Log some elements to help debug
                divs = soup.find_all('div', class_=lambda x: x and 'offer' in x)[:5]
                logging.info(f"Found {len(divs)} divs with 'offer' in class name")
                
                links = soup.find_all('a', href=True)[:10]
                for link in links:
                    logging.info(f"Sample link: {link.get('href', '')}")

            # Process job detail pages in parallel
            page_listings = []
            if urls:
                with ThreadPoolExecutor(max_workers=8) as pool:
                    fut2url = {pool.submit(self.get_page_html, u): u for u in urls}
                    for fut in as_completed(fut2url):
                        u = fut2url[fut]
                        try:
                            detail_html = fut.result(timeout=60)
                            job_listing = self._parse_job_detail(detail_html, u)
                            page_listings.append(job_listing)
                            
                            # Extract skills for this job
                            detail_soup = BeautifulSoup(detail_html, "html.parser")
                            skills = self._extract_skills_from_listing(detail_soup)
                            all_skills_dict[job_listing.job_id] = skills
                            
                        except Exception as ex:
                            logging.error(f"Error parsing {u}: {ex}")

            # Insert jobs into database
            for job in page_listings:
                if insert_job_listing(job):
                    successful_db_inserts += 1

            all_job_listings.extend(page_listings)
            
            logging.info(f"Processed {len(page_listings)} jobs on page {current_page}")

            # Save checkpoint after successfully processing this page
            self.save_checkpoint(current_page + 1)

            # Add delay before next page
            if current_page < end_page:
                time.sleep(random.uniform(2, 4))

            current_page += 1

        except Exception as e:
            logging.error(f"Error processing page {current_page}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            self.save_checkpoint(current_page + 1)
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
