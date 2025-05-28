from typing import List, Dict, Tuple, Set
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import re
import time
from ..models   import JobListing, Skill
from ..database import insert_job_listing, insert_skill
from .base_scraper import BaseScraper
import logging, re, random, time, os, tempfile, uuid
from datetime    import datetime
from typing      import Dict, List, Tuple, Optional, Set
from bs4         import BeautifulSoup
from ..models import JobListing
from .base_scraper import BaseScraper

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
            # Check if a checkpoint file exists in the temp directory
            checkpoint_path = os.path.join(tempfile.gettempdir(), "pracuj_checkpoint.txt")
            
            if os.path.exists(checkpoint_path):
                with open(checkpoint_path, "r") as f:
                    checkpoint_data = f.read().strip()
                    return int(checkpoint_data)
            return 1  # Default to page 1 if no checkpoint exists
        except Exception as e:
            logging.warning(f"Failed to retrieve checkpoint: {str(e)}")
            return 1  # Default to page 1 if an error occurs

    def save_checkpoint(self, page_number):
        """Save the current page as a checkpoint to a local file"""
        try:
            # Save checkpoint to a file in the temp directory
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
        
        # Remove non-breaking spaces and other characters
        clean_text = salary_text.replace('\xa0', '').replace('&nbsp;', '').replace(' ', '')
        
        # Check if it contains hourly rate indicators
        is_hourly = 'zł/h' in clean_text or 'zł/godz' in clean_text
        
        # Remove currency and other non-numeric characters
        clean_text = re.sub(r'[^\d,\.\-–]', '', clean_text)
        
        # Look for patterns like "12000–20000" or "150,00-180,00"
        match = re.search(r'([\d\.,]+)[–\-]([\d\.,]+)', clean_text)
        if match:
            try:
                # Handle both formats: "9000" and "150,00"
                min_val = match.group(1).replace(',', '.')
                max_val = match.group(2).replace(',', '.')
                
                # Convert to float first to handle decimal points
                min_salary = float(min_val)
                max_salary = float(max_val)
                
                # For hourly rates, convert to monthly (assuming 160 hours/month)
                if is_hourly:
                    min_salary = min_salary * 160
                    max_salary = max_salary * 160
                
                # Convert to integers for database storage
                return int(min_salary), int(max_salary)
            except ValueError:
                pass
                
        # Look for single value like "12000"
        match = re.search(r'([\d\.,]+)', clean_text)
        if match:
            try:
                # Handle both formats
                val = match.group(1).replace(',', '.')
                salary = float(val)
                
                # For hourly rates, convert to monthly
                if is_hourly:
                    salary = salary * 160
                
                # Convert to integer
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
            'employment_type': ''
        }
        
        # Get operating mode (work modes) - NEW SELECTOR
        operating_mode_elem = soup.find("li", attrs={"data-scroll-id": "work-modes", "class": "lowercase c196gesj"})
        if operating_mode_elem:
            result['operating_mode'] = operating_mode_elem.get_text(strip=True)
        
        # Get work type (work schedules) - NEW SELECTOR
        work_type_elem = soup.find("li", attrs={"data-test": "sections-benefit-work-schedule", 
                                            "data-scroll-id": "work-schedules"})
        if work_type_elem:
            result['work_type'] = work_type_elem.get_text(strip=True)
        
        # Get experience level - NEW SELECTOR
        experience_level_elem = soup.find("li", attrs={"data-test": "sections-benefit-employment-type-name", 
                                                    "data-scroll-id": "position-levels"})
        if experience_level_elem:
            result['experience_level'] = experience_level_elem.get_text(strip=True)
        
        # Get employment type - NEW SELECTOR
        employment_type_elem = soup.find("li", attrs={"data-test": "sections-benefit-contracts", 
                                                    "data-scroll-id": "contract-types"})
        if employment_type_elem:
            result['employment_type'] = employment_type_elem.get_text(strip=True)
        
        # Get location - using the old method as fallback
        location_elems = soup.find_all('div', attrs={'data-test': 'offer-badge-title'})
        for location_elem in location_elems:
            text = location_elem.text.strip()
            if any(x in text.lower() for x in ['warszawa', 'kraków', 'wrocław', 'gdańsk', 'poznań']):
                result['location'] = text
                break
        
        return result
    
    def _extract_skills_from_listing(self, soup: BeautifulSoup) -> List[str]:
        """Extract skills from the dedicated skills section and/or description"""
        found_skills = set()
        
        # FIRST PRIORITY: Check the dedicated skills section
        skills_section = soup.find("ul", attrs={"data-test": "aggregate-open-dictionary-model"})
        if skills_section:
            skill_items = skills_section.find_all("li", class_="catru5k")
            for item in skill_items:
                skill_text = item.get_text(strip=True).lower()
                # Add the skill directly - these are explicitly listed skills
                found_skills.add(skill_text)
                
        # SECOND PRIORITY: If few/no skills found, check the description bullets
        if len(found_skills) < 2:
            description_section = soup.find("ul", attrs={"data-test": "aggregate-bullet-model"})
            if description_section:
                bullet_items = description_section.find_all("li", class_="tkzmjn3")
                # Join all bullets into one description text
                description_text = " ".join([item.get_text(strip=True) for item in bullet_items])
                
                # Search for skills in this text
                desc_skills = self._extract_skills_from_text(description_text)
                found_skills.update(desc_skills)
        
        # Map raw skill texts to our standardized skills
        mapped_skills = self._map_to_standard_skills(found_skills)
        return list(mapped_skills)

    def _extract_skills_from_text(self, text: str) -> Set[str]:
        """Extract skills from description text"""
        # All possible skills from our categories
        all_skills = []
        for category, skills in self.skill_categories.items():
            all_skills.extend(skills)
            
        # Find skills in the text
        found_skills = set()
        text_lower = text.lower()
        
        for skill in all_skills:
            # Look for complete words/phrases, not partial matches
            pattern = r'\b{}\b'.format(re.escape(skill))
            if re.search(pattern, text_lower):
                found_skills.add(skill)
                
        return found_skills

    def _map_to_standard_skills(self, raw_skills: Set[str]) -> Set[str]:
        """Map raw skill texts to our standardized skill names"""
        mapped_skills = set()
        
        # All skill variations (including case variations, plurals, etc.)
        skill_variations = {
            # Database
            "sql": ["sql", "structured query language", "sql server", "t-sql", "pl/sql", "transact-sql", "język sql", "zapytania sql"],
            "mysql": ["mysql", "my sql", "maria db", "mariadb"],
            "postgresql": ["postgresql", "postgres", "postgre sql", "postgre", "psql"],
            "oracle": ["oracle", "oracle db", "oracle database", "baza oracle", "baza danych oracle"],
            "nosql": ["nosql", "no sql", "no-sql", "nierelacyjne bazy danych"],
            "mongodb": ["mongodb", "mongo", "mongo db"],
            "database": ["database", "baza danych", "bazy danych", "bd", "db", "rdbms"],
            "ms access": ["ms access", "microsoft access", "access", "msaccess"],
            "sqlite": ["sqlite", "sqlite3"],
            "redshift": ["redshift", "amazon redshift", "aws redshift"],
            "teradata": ["teradata", "tera data"],
    
            # Microsoft BI & Excel
            "excel": ["excel", "microsoft excel", "ms excel", "arkusz excel", "arkusze excel", "arkusze kalkulacyjne", "microsoft 365"],
            "power query": ["power query", "powerquery", "zapytania power query", "m code", "język m", "m language"],
            "power pivot": ["power pivot", "powerpivot", "tabele przestawne excel"],
            "vba": ["vba", "visual basic for applications", "makra excel", "excel macros", "visual basic", "kod vba"],
            "macros": ["macros", "makra", "makra excel", "excel macros"],
            "pivot tables": ["pivot tables", "tabele przestawne", "tabele pivot", "pivoty", "tabele pivotowe"],
            "excel formulas": ["excel formulas", "formuły excel", "funkcje excel", "wzory excel", "excel functions"],
            "ssrs": ["ssrs", "sql server reporting services", "reporting services"],
            "ssis": ["ssis", "sql server integration services", "integration services"],
            "ssas": ["ssas", "sql server analysis services", "analysis services"],
            "power apps": ["power apps", "powerapps", "microsoft power apps"],
            "power automate": ["power automate", "microsoft power automate", "flow", "microsoft flow"],
            
            # Visualization
            "power bi": ["power bi", "powerbi", "power-bi", "microsoft power bi", "ms power bi", "power bi desktop", "power bi service"],
            "tableau": ["tableau", "tableau desktop", "tableau server", "tableau online", "tableau prep"],
            "qlik": ["qlik", "qlikview", "qlik sense", "qlik sense enterprise"],
            "looker": ["looker", "google looker", "looker studio"],
            "data studio": ["data studio", "google data studio", "datastudio", "looker studio"],
            "dax": ["dax", "data analysis expressions", "wyrażenia analizy danych", "formuły dax", "funkcje dax"],
            "dashboard": ["dashboard", "dashboards", "pulpit", "pulpity", "kokpit", "panel analityczny", "panele analityczne"],
            "reporting": ["reporting", "raportowanie", "tworzenie raportów", "generowanie raportów"],
            
            # Programming
            "python": ["python", "język python", "python programming", "programowanie w python", "pythona"],
            "r": ["r", "język r", "r programming", "rstudio", "programowanie w r"],
            "java": ["java", "język java", "java programming", "programowanie w java"],
            "c#": ["c#", "c sharp", "csharp", "c-sharp", ".net c#"],
            ".net": [".net", ".net framework", ".net core", "dotnet", "dot net", "microsoft .net"],
            "javascript": ["javascript", "js", "język javascript", "es6", "ecmascript"],
            "pandas": ["pandas", "python pandas", "pd", "biblioteka pandas"],
            "numpy": ["numpy", "python numpy", "np", "biblioteka numpy"],
            "jupyter": ["jupyter", "jupyter notebook", "jupyter lab", "jupyterlab", "notatniki jupyter"],
            
            # Data Processing
            "etl": ["etl", "extract transform load", "ekstrakcja transformacja ładowanie", "procesy etl"],
            "spark": ["spark", "apache spark", "pyspark", "spark streaming", "spark sql"],
            "hadoop": ["hadoop", "apache hadoop", "hadoop ecosystem", "hdfs", "ekosystem hadoop"],
            "data cleansing": ["data cleansing", "czyszczenie danych", "oczyszczanie danych", "data cleaning"],
            "data warehouse": ["data warehouse", "hurtownia danych", "dwh", "data warehousing"],
            
            # Analytics & Statistics
            "statistics": ["statistics", "statystyka", "analizy statystyczne", "statistical analysis"],
            "regression": ["regression", "regresja", "analiza regresji", "regresja liniowa", "linear regression"],
            "forecasting": ["forecasting", "prognozowanie", "prognozy", "analiza szeregów czasowych", "time series forecasting"],
            "analytics": ["analytics", "analityka", "analiza danych", "data analytics"],
            "analysis": ["analysis", "analiza", "analizy", "analizowanie"],
            "spss": ["spss", "ibm spss", "spss statistics"],
            
            # Cloud
            "aws": ["aws", "amazon web services", "amazon aws", "ec2", "s3", "aws cloud"],
            "azure": ["azure", "microsoft azure", "azure cloud", "ms azure"],
            "gcp": ["gcp", "google cloud platform", "google cloud", "cloud platform"],
            "cloud": ["cloud", "chmura", "cloud computing", "przetwarzanie w chmurze"],
            "sharepoint": ["sharepoint", "microsoft sharepoint", "share point", "ms sharepoint"],
            
            # Business Intelligence
            "business intelligence": ["business intelligence", "bi", "analityka biznesowa", "inteligencja biznesowa"],
            "olap": ["olap", "on-line analytical processing", "analityczne przetwarzanie online", "kostki olap"],
            "kpi": ["kpi", "key performance indicator", "kluczowe wskaźniki efektywności", "wskaźniki kpi"],
            
            # Project Management and Soft Skills
            "project management": ["project management", "zarządzanie projektami", "pm", "project manager", "kierownik projektu"],
            "agile": ["agile", "agile methodology", "metodyka agile", "zwinne metodyki", "metodyki zwinne"],
            "scrum": ["scrum", "metodyka scrum", "scrum master", "scrum framework"],
            "jira": ["jira", "atlassian jira", "jira software"],
            "confluence": ["confluence", "atlassian confluence", "dokumentacja confluence"],
            "atlassian": ["atlassian", "narzędzia atlassian", "atlassian tools", "atlassian suite"],
            
            # Version Control
            "git": ["git", "system git", "kontrola wersji git", "git version control"],
            "github": ["github", "git hub", "serwis github"],
            
            # Data Integration and APIs
            "api": ["api", "application programming interface", "interfejs programistyczny aplikacji", "apis"],
            "rest api": ["rest api", "restful api", "restful", "rest apis", "restowe api"],
            "data integration": ["data integration", "integracja danych", "integrowanie danych", "systemy integracji"],
            "web scraping": ["web scraping", "screen scraping", "scraping", "ekstrakcja danych z www"],
            "ip rotation services": ["ip rotation services", "rotacja ip", "zmiana ip", "proxy rotation"],
            
            # ERP and CRM Systems
            "sap": ["sap", "sap erp", "system sap", "sap system"],
            "salesforce": ["salesforce", "sales force", "salesforce crm", "sf"],
            "dynamics": ["dynamics", "microsoft dynamics", "dynamics 365", "ms dynamics"],
            "erp": ["erp", "enterprise resource planning", "planowanie zasobów przedsiębiorstwa", "system erp"],
            "crm": ["crm", "customer relationship management", "zarządzanie relacjami z klientami", "system crm"]
        }
        
        # Map raw skills to standard names
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
        """Improved extraction of years of experience from requirements section"""
        # Try to find the requirements section
        requirements_section = soup.find("div", attrs={"data-test": "offer-sub-section", 
                                                      "data-scroll-id": "requirements-expected-1"})
        if not requirements_section:
            return None
        
        # Get the full text from the section
        requirements_text = requirements_section.get_text(strip=True).lower()
        
        # Look for experience patterns in both Polish and English
        patterns = [
            # Polish patterns
            r'(\d+)\s*rok\w*\s*doświadczeni',
            r'(\d+)\s*lat\w*\s*doświadczeni',
            r'doświadczeni\w*\s*(\d+)\s*rok',
            r'doświadczeni\w*\s*(\d+)\s*lat',
            r'doświadczeni\w*\s*min[.:]?\s*(\d+)\s*rok',
            r'doświadczeni\w*\s*min[.:]?\s*(\d+)\s*lat',
            r'min[.:]?\s*(\d+)\s*rok\w*\s*doświadczeni',
            r'min[.:]?\s*(\d+)\s*lat\w*\s*doświadczeni',
            r'(\d+)\+\s*rok\w*\s*doświadczeni',
            r'(\d+)\+\s*lat\w*\s*doświadczeni',
            r'(\d+)[-+](\d+)\s*lat\w*\s*doświadczeni',
            r'(\d+)[-+](\d+)\s*rok\w*\s*doświadczeni',
            
            # English patterns
            r'(\d+)\s*year\w*\s*experience',
            r'experience\s*of\s*(\d+)\s*year',
            r'min[.:]?\s*(\d+)\s*year\w*\s*experience',
            r'experience\s*min[.:]?\s*(\d+)\s*year',
            r'(\d+)\+\s*year\w*\s*experience',
            r'(\d+)[-+](\d+)\s*year\w*\s*experience'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, requirements_text)
            if match:
                try:
                    # If range like "3-5 years", take the minimum
                    return int(match.group(1))
                except (ValueError, IndexError):
                    pass
        
        return None
        
    def scrape(self) -> Tuple[List[JobListing], Dict[str, List[str]]]:
        # Initialize lists to store all results
        all_job_listings     = []
        all_skills_dict      = {}
        successful_db_inserts = 0
    
        # 1) Resume from last saved page
        last_processed_page = self.get_last_processed_page()
        current_page        = last_processed_page
        starting_page       = current_page

        # 2) Auto-detect how many pages exist right now (parse description → buttons → URL)
        first_html   = self.get_page_html(self.search_url)
        first_soup   = BeautifulSoup(first_html, "html.parser")

        total_pages = None

        # a) Try the "Strona X z Y" description
        page_desc = first_soup.find(text=re.compile(r"Strona\s+\d+\s+z\s+\d+"))
        if page_desc:
            m_desc = re.search(r"Strona\s+\d+\s+z\s+(\d+)", page_desc)
            if m_desc:
                total_pages = int(m_desc.group(1))
                self.logger.info(f"Detected total_pages={total_pages} from description: '{page_desc.strip()}'")  # 

        # b) Fallback → look for any numbered pagination buttons
        if total_pages is None:
            nums = []
            # catch any <a> or <button> whose text is a digit
            for el in first_soup.select("a, button"):
                txt = el.get_text(strip=True)
                if txt.isdigit():
                    nums.append(int(txt))
            if nums:
                total_pages = max(nums)
                self.logger.info(f"Detected total_pages={total_pages} from buttons: {sorted(set(nums))}")

        # c) Fallback → catch any '?pn=N' in hrefs
        if total_pages is None:
            nums = []
            for a in first_soup.find_all("a", href=True):
                m = re.search(r"[?&]pn=(\d+)", a["href"])
                if m:
                    nums.append(int(m.group(1)))
            if nums:
                total_pages = max(nums)
                self.logger.info(f"Detected total_pages={total_pages} from URL params: {sorted(set(nums))}")

        # d) If still missing, assume just one page
        if total_pages is None:
            total_pages = 1
            self.logger.warning("Could not detect pagination; defaulting to total_pages=1")

        # 3) Pick which pages to scrape this run
        pages_per_run = total_pages
        end_page      = min(starting_page + pages_per_run - 1, total_pages)

        self.logger.info(f"Scraping pages {starting_page}–{end_page} of {total_pages}")
    
        # Track processed URLs to prevent duplicates
        processed_urls = set()
    
        logging.info(f"Scraping pages {current_page}–{end_page} of {total_pages}")
             
        while current_page <= end_page:
            self.logger.info(f"Processing page {current_page} of {end_page}")
            # Modify URL for pagination
            page_url = f"{self.search_url}" if current_page == 1 else f"{self.search_url}&pn={current_page}"

            # 1) GET the search-results page
            html = self.get_page_html(page_url)
            soup = BeautifulSoup(html, "html.parser")
            job_containers = soup.select("li.offer")  # or your existing selector

            # 2) Collect all new job URLs
            job_urls = []
            for c in job_containers:
                url = c.select_one("a.offer-link")["href"]
                if "pracodawcy.pracuj.pl/company" in url or url in processed_urls:
                    self.logger.info(f"Skipping company ad URL: {url}")
                    continue
                job_urls.append(url)
                processed_urls.add(url)

            # 3) Fetch & parse detail pages in parallel
            listings = []
            with ThreadPoolExecutor(max_workers=8) as pool:
                future_to_url = {pool.submit(self.get_page_html, u): u for u in job_urls}
                for fut in as_completed(future_to_url):
                    u = future_to_url[fut]
                    try:
                        detail_html = fut.result(timeout=60)
                        listing    = self._parse_job_detail(detail_html, u)
                        listings.append(listing)
                    except Exception as e:
                        self.logger.error(f"Error fetching/parsing {u}: {e}")

            # 4) Bulk-insert into DB
            for job in listings:
                if insert_job_listing(job):
                    successful_db_inserts += 1

            # checkpoint & advance
            save_checkpoint(current_page + 1)
            current_page += 1
            time.sleep(random.uniform(2, 4))
            
            try:
                # Get search results page
                logging.info(f"Fetching search results from {page_url}")
                html = self.get_page_html(page_url)
                if not html:
                    logging.error(f"Failed to fetch search results page {current_page}")
                    # Save checkpoint to next page so we don't retry forever
                    self.save_checkpoint(current_page + 1)
                    current_page += 1
                    continue
                    
                soup = BeautifulSoup(html, "html.parser")
                
                # Find the main container with all job offers
                offers_container = soup.find("div", attrs={"data-test": "section-offers"})
                if not offers_container:
                    logging.warning(f"Main offers container not found on page {current_page}")
                    # Try fallback to the previous selectors
                    job_containers = soup.select("#offers-list > div.listing_b1i2dnp8 > div.listing_ohw4t83")
                    if not job_containers:
                        job_containers = soup.select("div.listing_ohw4t83")
                        if not job_containers:
                            job_containers = soup.find_all("div", class_=lambda c: c and "listing_" in c)
                else:
                    # Find all article elements within the offers container - these are individual job listings
                    job_containers = offers_container.find_all("article")
                    if not job_containers:
                        # If no articles found, try div elements that might contain job listings
                        job_containers = offers_container.find_all("div", recursive=False)
                
                logging.info(f"Found {len(job_containers)} job listings on page {current_page}")
                
                # If no jobs found on this page, stop pagination
                if not job_containers:
                    logging.info(f"No jobs found on page {current_page}. Stopping pagination.")
                    # Save checkpoint to next page so we don't retry this page
                    self.save_checkpoint(current_page + 1)
                    break
                
                # Process jobs from this page
                page_job_listings = []
                page_skills_dict = {}
                errors = 0
                
                for job_container in job_containers:
                    try:
                        # Find the job URL - try multiple approaches
                        job_url = None
                        
                        # Method 1: Look for any link with job-like keywords in the URL
                        all_links = job_container.find_all("a", href=True)
                        for link in all_links:
                            href = link["href"]
                            if any(keyword in href.lower() for keyword in ["praca/", "oferta/", "job/", "offer/"]):
                                job_url = href
                                break
                        
                        # Method 2: If no job-specific links found, try links with titles or header elements
                        if not job_url:
                            for link in all_links:
                                # Check if the link contains a header element
                                if link.find(["h1", "h2", "h3", "h4"]) or link.get("title"):
                                    job_url = link["href"]
                                    break
                        
                        # Method 3: Last resort - take the first link with a non-empty href
                        if not job_url and all_links:
                            job_url = all_links[0]["href"]
                        
                        # Skip if no link found or validate the URL
                        if not job_url:
                            logging.info("Skipping job - could not find any usable link")
                            continue

                        if not job_url.startswith("http"):
                            job_url = self.base_url + job_url
                            
                        # Skip if we've already processed this URL
                        if job_url in processed_urls:
                            logging.info(f"Skipping duplicate job URL: {job_url}")
                            continue
                            
                        processed_urls.add(job_url)
                        
                        logging.info(f"Processing job: {job_url}")
                        
                        # Now fetch the job detail page to get more information
                        job_detail_html = self.get_page_html(job_url)
                        if not job_detail_html:
                            continue
                            
                        detail_soup = BeautifulSoup(job_detail_html, "html.parser")
                        
                        # Extract job title
                        title_element = detail_soup.find("h1", attrs={"data-test": "text-positionName"})
                        job_title = title_element.text.strip() if title_element else "Unknown Title"
                        
                        # Extract company name
                        company_element = detail_soup.find("h2", attrs={"data-test": "text-employerName"})
                        if company_element:
                            # Remove the 'O firmie' link if present
                            company_link = company_element.find("a")
                            if company_link:
                                company_link.decompose()
                            company = company_element.text.strip()
                        else:
                            company = "Unknown Company"
                            
                        # Skip company ads without title/name
                        if job_title == "Unknown Title" and company == "Unknown Company":
                            logging.info(f"Skipping company ad URL: {job_url}")
                            continue
                        
                        # Extract salary if available
                        salary_element = detail_soup.find("div", attrs={"data-test": "text-earningAmount"})
                        salary_text = salary_element.text.strip() if salary_element else ""
                        salary_min, salary_max = self._extract_salary(salary_text)
                        
                        # Extract all badge information
                        badge_info = self._extract_badge_info(detail_soup)
                        
                        # Extract years of experience
                        years_of_experience = self._extract_years_of_experience(detail_soup)
                        
                        # Extract skills using our more precise method
                        extracted_skills = self._extract_skills_from_listing(detail_soup)
                        
                        # Generate a unique job ID 
                        external_id_match = re.search(r',oferta,(\d+)', job_url)
                        job_id = external_id_match.group(1) if external_id_match else job_url
                        
                        # Create job listing object (including an empty description field)
                        job = JobListing(
                            job_id=job_id,
                            source="pracuj.pl",
                            title=job_title,
                            company=company,
                            link=job_url,
                            salary_min=salary_min,
                            salary_max=salary_max,
                            location=badge_info['location'],
                            operating_mode=badge_info['operating_mode'],
                            work_type=badge_info['work_type'],
                            experience_level=badge_info['experience_level'],
                            employment_type=badge_info['employment_type'],
                            years_of_experience=years_of_experience,
                            scrape_date=datetime.now(),
                            listing_status="Active"
                        )
                        
                        # Insert the job into the database
                     
                      
                        job_db_id = insert_job_listing(job)
                        if job.short_id:  # only “new” insertions get a short_id set
                            logging.info(f"✅ Inserted new job: {job_title} as ID {job_db_id}")
                            successful_db_inserts += 1
                            page_job_listings.append(job)
                            page_skills_dict[job_id] = extracted_skills
                        else:
                            logging.info(f"🔄 Job already existed, skipping append: {job_title}")
                  
                    except Exception as e:
                        errors += 1
                        logging.error(f"Error processing job element: {str(e)}")
                        import traceback
                        logging.error(traceback.format_exc())
                        continue
                
                # Add results from this page to main collection
                all_job_listings.extend(page_job_listings)
                all_skills_dict.update(page_skills_dict)
                
                logging.info(f"Processed {len(page_job_listings)} jobs with {errors} errors on page {current_page}")
                
                # Save checkpoint after successfully processing this page
                self.save_checkpoint(current_page + 1)
                
                # Add random delay before fetching next page - use shorter delays
                if current_page < end_page:  # Only delay if not on the last page
                    page_delay = 2 + random.uniform(0, 2)
                    logging.info(f"Waiting {page_delay:.2f} seconds before fetching next page")
                    time.sleep(page_delay)
                
                # Move to next page
                current_page += 1
                
            except Exception as e:
                logging.error(f"Error processing page {current_page}: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())
                
                # Save checkpoint to the next page even if there was an error
                self.save_checkpoint(current_page + 1)
                current_page += 1
        
        # Final summary logs
        logging.info(f"Scrape summary: Processed {len(all_job_listings)} jobs across {current_page - starting_page} pages")
        logging.info(f"Next run will start from page {current_page}")
        logging.info(f"Jobs successfully inserted in database: {successful_db_inserts}")
        
        return all_job_listings, all_skills_dict 

    def _parse_job_detail(self, html: str, job_url: str) -> JobListing:
        """Extract a JobListing from a detail-page HTML."""
        soup = BeautifulSoup(html, "html.parser")

        # stable ID from URL
        m = re.search(r',oferta,(\d+)', job_url)
        job_id = m.group(1) if m else job_url

        # title + company
        title   = soup.select_one("h1.offer-title").get_text(strip=True)
        company = soup.select_one("a.company-name").get_text(strip=True)

        # badges (reuse your existing badge-parsing logic)
        badges = self._parse_badges(soup)

        # optional: years-of-experience text
        yoe_txt = soup.find(text=re.compile(r"(\d+)\s+years?"))
        yoe = int(re.search(r"(\d+)", yoe_txt).group(1)) if yoe_txt else None

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

def scrape_pracuj():
    """Function to run the pracuj.pl scraper"""
    scraper = PracujScraper()
    return scraper.scrape()
