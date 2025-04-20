
import logging
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Set
import uuid

from bs4 import BeautifulSoup
from ..models import JobListing
from .base_scraper import BaseScraper

class PracujScraper(BaseScraper):
    """Scraper for pracuj.pl job board"""
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.pracuj.pl"
        self.search_url = "https://it.pracuj.pl/praca/warszawa;wp?rd=30&et=17%2C4%2C3&its=business-analytics%2Cbig-data-science"
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
    
    def _extract_salary(self, salary_text: str) -> Tuple[Optional[int], Optional[int]]:
        """Extract min and max salary from salary text"""
        if not salary_text:
            return None, None
            
        # Remove non-breaking spaces and other characters
        clean_text = salary_text.replace('\xa0', '').replace('&nbsp;', '').replace(' ', '')
        
        # Look for patterns like "12000–20000"
        match = re.search(r'(\d+)[–-](\d+)', clean_text)
        if match:
            try:
                min_salary = int(match.group(1))
                max_salary = int(match.group(2))
                return min_salary, max_salary
            except ValueError:
                pass
                
        # Look for single value like "12000"
        match = re.search(r'(\d+)', clean_text)
        if match:
            try:
                salary = int(match.group(1))
                return salary, salary
            except ValueError:
                pass
                
        return None, None
    
    def _extract_badge_info(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract information from badge elements that all use the same selector"""
        result = {
            'location': '',
            'operating_mode': '',
            'work_type': '',
            'experience_level': '',
            'employment_type': ''
        }
        
        # Get all badge elements
        badges = soup.find_all('div', attrs={'data-test': 'offer-badge-title'})
        
        for badge in badges:
            text = badge.text.strip()
            
            # Categorize based on content
            if any(x in text.lower() for x in ['warszawa', 'kraków', 'wrocław', 'gdańsk', 'poznań']):
                result['location'] = text
            elif any(x in text.lower() for x in ['stacjonarna', 'zdalna', 'hybrydowa']):
                result['operating_mode'] = text
            elif any(x in text.lower() for x in ['pełny etat', 'część etatu']):
                result['work_type'] = text
            elif any(x in text.lower() for x in ['specjalista', 'młodszy', 'asystent', 'junior', 'mid', 'senior']):
                result['experience_level'] = text
            elif any(x in text.lower() for x in ['umowa o pracę', 'kontrakt b2b', 'umowa zlecenie', 'umowa o dzieło', 'umowa o pracę tymczasową']):
                result['employment_type'] = text
                
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
        """Try to extract years of experience from the description bullets"""
        description_section = soup.find("ul", attrs={"data-test": "aggregate-bullet-model"})
        if not description_section:
            return None
            
        bullet_items = description_section.find_all("li", class_="tkzmjn3")
        for item in bullet_items:
            text = item.get_text(strip=True).lower()
            
            # Look for patterns like "X lat doświadczenia" or "doświadczenie: X lat"
            patterns = [
                r'(\d+)\s*lat\w*\s*doświadczeni',
                r'doświadczeni\w*\s*(\d+)\s*lat',
                r'doświadczeni\w*\s*min[.:]?\s*(\d+)\s*lat',
                r'min[.:]?\s*(\d+)\s*lat\w*\s*doświadczeni',
                r'(\d+)\+\s*lat\w*\s*doświadczeni',
                r'(\d+)[-+](\d+)\s*lat\w*\s*doświadczeni'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    try:
                        # If range like "3-5 years", take the minimum
                        return int(match.group(1))
                    except (ValueError, IndexError):
                        pass
        
        return None
        
    def scrape(self) -> Tuple[List[JobListing], Dict[str, List[str]]]:
        """Scrape job listings from pracuj.pl"""
        job_listings = []
        skills_dict = {}
        
        try:
            # Get search results page
            logging.info(f"Fetching search results from {self.search_url}")
            html = self.get_page_html(self.search_url)
            if not html:
                logging.error("Failed to fetch search results page")
                return job_listings, skills_dict
                
            soup = BeautifulSoup(html, "html.parser")
            
            # Find all job listings on the page using the provided selector
            job_containers = soup.select("#offers-list > div.listing_b1i2dnp8 > div.listing_ohw4t83")
            if not job_containers:
                # Try a more general selector if the specific one fails
                job_containers = soup.select("div.listing_ohw4t83")
                
            logging.info(f"Found {len(job_containers)} job listings on the page")
            
            for job_container in job_containers:
                try:
                    # Find the link to the job details
                    link_container = job_container.select_one("div.tiles_cobg3mp")
                    if not link_container:
                        continue
                        
                    # Find the actual link element
                    link_element = link_container.find("a", href=True)
                    if not link_element:
                        continue
                        
                    job_url = link_element.get("href")
                    if not job_url.startswith("http"):
                        job_url = self.base_url + job_url
                    
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
                    
                    # Extract salary if available
                    salary_element = detail_soup.find("div", attrs={"data-test": "text-earningAmount"})
                    salary_text = salary_element.text.strip() if salary_element else ""
                    salary_min, salary_max = self._extract_salary(salary_text)
                    
                    # Extract all badge information
                    badge_info = self._extract_badge_info(detail_soup)
                    
                    # Extract full description
                    description_elements = detail_soup.find_all("div", class_=lambda c: c and "offer_" in c)
                    description_texts = []
                    for elem in description_elements:
                        if elem.get_text(strip=True):
                            description_texts.append(elem.get_text(strip=True))
                    description = "\n".join(description_texts)
                    
                    # Extract years of experience
                    years_of_experience = self._extract_years_of_experience(detail_soup)
                    
                    # Extract skills using our more precise method
                    extracted_skills = self._extract_skills_from_listing(detail_soup)
                    
                    # Generate a unique job ID 
                    job_id = str(uuid.uuid4())
                    
                    # Create job listing object
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
                        description=description,
                        scrape_date=datetime.now(),
                        listing_status="Active"
                    )
                    
                    job_listings.append(job)
                    skills_dict[job_id] = extracted_skills
                    
                except Exception as e:
                    logging.error(f"Error processing job element: {str(e)}")
                    import traceback
                    logging.error(traceback.format_exc())
                    continue
            
            return job_listings, skills_dict
            
        except Exception as e:
            logging.error(f"Error in pracuj.pl scraper: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return job_listings, skills_dict
            
    def scrape(self) -> Tuple[List[JobListing], Dict[str, List[str]]]:
        """Scrape job listings from pracuj.pl"""
        # Initialize lists to store all results
        all_job_listings = []
        all_skills_dict = {}
        
        # Process multiple pages
        max_pages = 5  # Adjust as needed
        current_page = 1
        
        while current_page <= max_pages:
            logging.info(f"Processing page {current_page} of {max_pages}")
            # Modify URL for pagination
            page_url = f"{self.search_url}" if current_page == 1 else f"{self.search_url}&pn={current_page}"
            
            try:
                # Get search results page
                logging.info(f"Fetching search results from {page_url}")
                html = self.get_page_html(page_url)
                if not html:
                    logging.error(f"Failed to fetch search results page {current_page}")
                    current_page += 1
                    continue
                    
                soup = BeautifulSoup(html, "html.parser")
                
                # Find all job listings on the page using the provided selector
                job_containers = soup.select("#offers-list > div.listing_b1i2dnp8 > div.listing_ohw4t83")
                if not job_containers:
                    # Try a more general selector if the specific one fails
                    job_containers = soup.select("div.listing_ohw4t83")
                    if not job_containers:
                        job_containers = soup.find_all("div", class_=lambda c: c and "listing_" in c)
                
                logging.info(f"Found {len(job_containers)} job listings on page {current_page}")
                
                # If no jobs found on this page, stop pagination
                if not job_containers:
                    logging.info(f"No jobs found on page {current_page}. Stopping pagination.")
                    break
                
                # Process jobs from this page
                page_job_listings = []
                page_skills_dict = {}
                errors = 0
                
                for job_container in job_containers:
                    try:
                        # Find the link to the job details
                        link_container = job_container.select_one("div.tiles_cobg3mp")
                        if not link_container:
                            continue
                            
                        # Find the actual link element
                        link_element = link_container.find("a", href=True)
                        if not link_element:
                            continue
                            
                        job_url = link_element.get("href")
                        if not job_url.startswith("http"):
                            job_url = self.base_url + job_url
                        
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
                        
                        # Extract salary if available
                        salary_element = detail_soup.find("div", attrs={"data-test": "text-earningAmount"})
                        salary_text = salary_element.text.strip() if salary_element else ""
                        salary_min, salary_max = self._extract_salary(salary_text)
                        
                        # Extract all badge information
                        badge_info = self._extract_badge_info(detail_soup)
                        
                        # Extract full description
                        description_elements = detail_soup.find_all("div", class_=lambda c: c and "offer_" in c)
                        description_texts = []
                        for elem in description_elements:
                            if elem.get_text(strip=True):
                                description_texts.append(elem.get_text(strip=True))
                        description = "\n".join(description_texts)
                        
                        # Extract years of experience
                        years_of_experience = self._extract_years_of_experience(detail_soup)
                        
                        # Extract skills using our more precise method
                        extracted_skills = self._extract_skills_from_listing(detail_soup)
                        
                        # Generate a unique job ID 
                        job_id = str(uuid.uuid4())
                        
                        # Create job listing object
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
                            description=description,
                            scrape_date=datetime.now(),
                            listing_status="Active"
                        )
                        
                        page_job_listings.append(job)
                        page_skills_dict[job_id] = extracted_skills
                        
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
                
                # Move to next page
                current_page += 1
                
            except Exception as e:
                logging.error(f"Error processing page {current_page}: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())
                current_page += 1
        
        logging.info(f"Total jobs collected: {len(all_job_listings)}")
        return all_job_listings, all_skills_dict 
  
def scrape_pracuj():
    """Function to run the pracuj.pl scraper"""
    scraper = PracujScraper()
    return scraper.scrape()
