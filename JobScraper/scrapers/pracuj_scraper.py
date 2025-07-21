import csv
import json
import logging
import os
import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class PracujScraper:
    """
    A class to scrape job offers from pracuj.pl, process them, and save to CSV.
    """

    def __init__(self, output_filename="job_offers.csv"):
        """
        Initializes the PracujScraper with skill categories and other attributes.
        """
        self.base_url = "https://it.pracuj.pl/praca?et=3%2C17%2C4&its=big-data-science"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
        }
        self.jobs_data = []
        self.output_filename = output_filename
        self.processed_urls = self.load_processed_urls()
        # --- Skill Categories ---
        self.skill_categories = {
            "Database": ["sql", "mysql", "postgresql", "oracle", "nosql", "mongodb", "database", "ms access", "sqlite", "redshift", "snowflake", "microsoft sql server", "teradata", "clickhouse", "azure sql database", "azure sql managed instance", "mariadb", "ms sql", "sql i pl/sql", "oracle forms", "oracle apex", "oracle ebs", "oracle application framework (oaf)", "oracle erp cloud", "sql server", "mssqlserver", "azure sql", "pl/pgsql", "aas", "neteza", "singlestore", "oracle fusion middleware", "oracle jdeveloper"],
            "Microsoft BI & Excel": ["excel", "power query", "power pivot", "vba", "macros", "pivot tables", "excel formulas", "spreadsheets", "m code", "ssrs", "ssis", "ssas", "power apps", "power automate", "powerpoint", "office 365", "microsoft power bi", "power bi", "power bi.", "ms office", "ms excel", "microsoft dynamics 365", "ms fabric"],
            "Visualization": ["tableau", "qlik", "looker", "data studio", "powerbi", "dax", "matplotlib", "seaborn", "plotly", "excel charts", "dashboard", "reporting", "d3.js", "grafana", "kibana", "google charts", "quicksight", "sas viya", "di studio", "eg", "sas studio", "visual analytics", "qliksense", "sas va", "qgis", "visio"],
            "Programming": ["python", "r", "java", "scala", "c#", ".net", "javascript", "typescript", "pandas", "numpy", "jupyter", "scikit-learn", "tidyverse", "julia", "sql scripting", "pl/sql", "t-sql", "linux", "windows", "unix", "windows server", "macos", "shell", "perl", "pyspark", "go", "rust", "c++", "c", "jee", "scala 3", "next.js", "fastapi", "rest", "spring framework", "css", "html", "u-boot", "yocto", "sas4gl", "mql5", "xml", "uml", "bpmn", "golang", "graphql", "spring boot", "hibernate", "flask api", "pytest", "junit", "liquibase", "jest", "angular", "vue.js", "ngrx", "swagger"],
            "Data Processing": ["etl", "spark", "hadoop", "kafka", "airflow", "data engineering", "big data", "data cleansing", "data transformation", "data modeling", "data warehouse", "databricks", "dbt", "talend", "informatica", "apache spark", "starrocks", "iceberg", "bigquery", "matillion", "data built tool", "apache airflow", "data lake", "adf", "azure data factory", "azure data lake", "parquet", "dwh", "elt/elt", "apache kafka", "alteryx", "azure databricks", "synapse analytics", "informatica cloud"],
            "Analytics & Statistics": ["statistics", "regression", "forecasting", "analytics", "analysis", "spss", "sas", "stata", "hypothesis testing", "a/b testing", "statistical", "time series", "clustering", "segmentation", "correlation", "adobe analytics", "google analytics", "sas di", "sas eg", "sas 4gl", "sas macro language", "data science", "data analytics"],
            "Cloud": ["aws", "azure", "gcp", "google cloud", "cloud", "onedrive", "sharepoint", "snowflake", "lambda", "s3", "pub/sub", "dataflow", "terraform", "google cloud services (big query)", "microsoft azure", "snowflake data cloud", "google cloud platform", "sap datasphere", "azure synapse", "azure functions", "azure repos", "microsoft  azure", "redis", "azure event hub", "ansible", "terragrunt", "vertex ai", "sagemaker", "azure devops"],
            "Business Intelligence": ["business intelligence", "bi", "cognos", "business objects", "microstrategy", "olap", "data mart", "reporting", "kpi", "metrics", "domo", "sisense", "bi publisher", "mis"],
            "Machine Learning and AI": ["machine learning", "scikit-learn", "tensorflow", "keras", "pytorch", "deep learning", "xgboost", "lightgbm", "nlp", "computer vision", "anomaly detection", "feature engineering", "opencv", "langchain", "pydantic", "langgraph", "hugging face ml tools", "mlops", "dagster", "llm", "ai", "ml", "transformers", "openai api", "tensorrt", "seldon", "onnx", "cap n proto", "llamaindex", "mlflow", "kubeflow", "vllm", "pinecone", "faiss", "chroma", "llm/nlp", "sciklit-learn", "palantir foundry"],
            "Data Governance and Quality": ["data governance", "data quality", "data integrity", "data validation", "master data management", "metadata", "data lineage", "data catalog", "atlan", "collibra", "cdi", "cai", "cdgc"],
            "Data Privacy and Security": ["data privacy", "gdpr", "data security", "compliance", "pii", "data anonymization"],
            "Project Management and Soft Skills": ["project management", "agile", "scrum", "communication", "presentation", "storytelling", "collaboration", "stakeholder management", "requirements gathering", "jira", "confluence", "agile methodologies", "servicenow", "bugzilla", "otrs"],
            "Version Control": ["git", "github", "gitlab", "bitbucket", "svn"],
            "Data Integration and APIs": ["api", "rest api", "data integration", "web scraping", "etl tools", "soap", "ip rotation services", "google python apis", "rest apis", "soapui", "oracle service bus", "oracle soa"],
            "ERP and CRM Systems": ["sap", "oracle", "salesforce", "dynamics", "erp", "crm", "workday"],
            "DevOps": ["jenkins", "openshift", "docker", "kubernetes", "bamboo", "ci/cd", "maven", "gradle", "sonarqube", "argocd", "jenkins / ansible", "controlm", "liquiibase", "sonar"]
        }

    def load_processed_urls(self):
        """
        Loads already processed URLs from the CSV file to avoid duplicates.
        """
        if not os.path.exists(self.output_filename):
            return set()
        try:
            df = pd.read_csv(self.output_filename)
            return set(df["Job Offer URL"].tolist())
        except pd.errors.EmptyDataError:
            return set()

    def get_total_pages(self):
        """
        Determines the total number of pages to scrape.
        """
        try:
            response = requests.get(self.base_url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            # Find the script tag with id '__NEXT_DATA__'
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
            if not script_tag:
                logging.warning("Could not find the '__NEXT_DATA__' script tag.")
                return 1
            json_data = json.loads(script_tag.string)
            # Navigate through the JSON structure to get page count
            page_count = json_data.get('props', {}).get('pageProps', {}).get('pageCount')
            if page_count:
                logging.info(f"Total pages to scrape: {page_count}")
                return page_count
            else:
                logging.warning("Could not determine total number of pages. Scraping only the first page.")
                return 1
        except requests.RequestException as e:
            logging.error(f"Error fetching total pages: {e}")
            return 1
        except (ValueError, KeyError) as e:
            logging.error(f"Error parsing JSON for page count: {e}")
            return 1

    def scrape_page(self, page_num):
        """
        Scrapes a single page of job offers.
        """
        page_url = f"{self.base_url}&pn={page_num}"
        try:
            response = requests.get(page_url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            # Find the script tag with id '__NEXT_DATA__'
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
            if not script_tag:
                logging.warning(f"Could not find the '__NEXT_DATA__' script tag on page {page_num}.")
                return
            json_data = json.loads(script_tag.string)
            # Navigate to the job offers list
            job_offers = json_data.get('props', {}).get('pageProps', {}).get('jobOffers', [])
            if not job_offers:
                logging.warning(f"No job offers found on page {page_num}.")
                return

            for job in job_offers:
                job_url = job.get('offerUrl')
                if job_url and job_url not in self.processed_urls:
                    self.scrape_job_details(job_url)
                elif job_url:
                    logging.info(f"Skipping already processed job: {job_url}")

        except requests.RequestException as e:
            logging.error(f"Error scraping page {page_num}: {e}")
        except (ValueError, KeyError) as e:
            logging.error(f"Error parsing JSON on page {page_num}: {e}")

    def scrape_job_details(self, job_url):
        """
        Scrapes details from a single job offer page.
        """
        try:
            response = requests.get(job_url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

            if not script_tag:
                logging.warning(f"Could not find the '__NEXT_DATA__' script tag on job page: {job_url}")
                return

            json_data = json.loads(script_tag.string)
            job_details = json_data.get('props', {}).get('pageProps', {}).get('jobOffer', {})

            if not job_details:
                logging.warning(f"No job details found for URL: {job_url}")
                return

            # Extracting the required details from the JSON object
            job_title = job_details.get('jobTitle', 'N/A')
            company_name = job_details.get('companyName', 'N/A')
            work_mode = job_details.get('workModes', [{}])[0].get('label', 'N/A')
            employment_types = ', '.join([e.get('label', '') for e in job_details.get('employmentTypes', [])])
            experience_level = ', '.join([e.get('label', '') for e in job_details.get('levels', [])])
            contract_type = ', '.join([e.get('label', '') for e in job_details.get('contracts', [])])

            # Address extraction is complex due to various possible structures
            workplace = job_details.get('workplaces', [{}])[0]
            city = workplace.get('city', 'N/A')
            street = workplace.get('street', {}).get('label')
            address = f"{street}, {city}" if street else city

            # Main technologies / skills extraction
            tech_stack = [tech.get('label', '') for tech in job_details.get('technologies', [])]
            offer_text = ' '.join([str(val) for val in job_details.values()]).lower()

            categorized_skills = self.categorize_skills(tech_stack, offer_text)

            current_date = datetime.now().strftime("%Y-%m-%d")

            job_data = {
                "Date Scraped": current_date,
                "Job Title": job_title,
                "Company Name": company_name,
                "City": city,
                "Address": address,
                "Work Mode": work_mode,
                "Employment Type": employment_types,
                "Experience Level": experience_level,
                "Contract Type": contract_type,
                "Job Offer URL": job_url,
                **categorized_skills
            }
            self.jobs_data.append(job_data)
            self.processed_urls.add(job_url)  # Add to processed URLs
            logging.info(f"Successfully scraped: {job_title} at {company_name}")

        except requests.RequestException as e:
            logging.error(f"Error scraping job details from {job_url}: {e}")
        except (ValueError, KeyError) as e:
            logging.error(f"Error parsing job details JSON from {job_url}: {e}")

    def categorize_skills(self, tech_stack, offer_text):
        """
        Categorizes skills based on predefined lists.
        """
        categorized = {category: [] for category in self.skill_categories}
        all_skills = set(skill.lower() for skill in tech_stack)

        # Simple text search for skills in the offer text
        for category, skills in self.skill_categories.items():
            for skill in skills:
                if f' {skill} ' in f' {offer_text} ':
                    all_skills.add(skill)

        # Categorize the found skills
        for skill in all_skills:
            for category, skill_list in self.skill_categories.items():
                if skill in skill_list:
                    categorized[category].append(skill)
                    break  # Move to the next skill once categorized

        # Convert lists to comma-separated strings
        for category in categorized:
            categorized[category] = ', '.join(sorted(list(set(categorized[category]))))

        return categorized

    def save_to_csv(self):
        """
        Saves the scraped data to a CSV file.
        """
        if not self.jobs_data:
            logging.info("No new job offers to save.")
            return

        # Prepare headers: standard fields + all skill categories
        fieldnames = [
                         "Date Scraped", "Job Title", "Company Name", "City", "Address", "Work Mode",
                         "Employment Type", "Experience Level", "Contract Type", "Job Offer URL"
                     ] + sorted(list(self.skill_categories.keys()))

        file_exists = os.path.exists(self.output_filename)
        try:
            with open(self.output_filename, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists or os.path.getsize(self.output_filename) == 0:
                    writer.writeheader()
                writer.writerows(self.jobs_data)
            logging.info(f"Successfully appended {len(self.jobs_data)} new job offers to {self.output_filename}")
        except IOError as e:
            logging.error(f"Error saving data to CSV: {e}")

    def run(self):
        """
        Runs the full scraping process.
        """
        logging.info("Starting the scraper.")
        total_pages = self.get_total_pages()
        if total_pages > 0:
            for page_num in range(1, total_pages + 1):
                logging.info(f"Scraping page {page_num}/{total_pages}...")
                self.scrape_page(page_num)
            self.save_to_csv()
        logging.info("Scraping finished.")


# Main execution block
if __name__ == "__main__":
    scraper = PracujScraper()
    scraper.run()
