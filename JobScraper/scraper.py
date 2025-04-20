import logging
from datetime import datetime
from typing import List
import traceback

from .database import create_tables_if_not_exist, insert_job_listing, insert_skill
from .models import JobListing, Skill

# Import individual scrapers
from .scrapers.pracuj_scraper import scrape_pracuj
# Add more scrapers as they are implemented
# from .scrapers.justjoin_scraper import scrape_justjoin
# from .scrapers.nofluffjobs_scraper import scrape_nofluffjobs

def process_skills(job_listing: JobListing, skills_list: List[str], skill_categories: dict) -> None:
    """Process and save skills for a job listing"""
    for skill_name in skills_list:
        # Determine skill category based on predefined mapping
        category = "Other"
        for cat, skills in skill_categories.items():
            if skill_name.lower() in skills:
                category = cat
                break
                
        # Create and save skill
        skill = Skill(
            job_id=job_listing.job_id,
            source=job_listing.source,
            skill_name=skill_name,
            skill_category=category
        )
        
        insert_skill(skill)
    
def run_scraper():
    """Main function to coordinate all scraping activities"""
    start_time = datetime.now()
    logging.info(f"Starting job scraper at {start_time}")
    
    # Get skill categories from the pracuj scraper to ensure consistency
    from .scrapers.pracuj_scraper import PracujScraper
    temp_scraper = PracujScraper()
    skill_categories = temp_scraper.skill_categories
    
    # Make sure database tables exist
    if not create_tables_if_not_exist():
        logging.error("Failed to create database tables. Exiting.")
        return
    
    # Initialize counters
    total_jobs = 0
    total_skills = 0
    errors = 0
    
    # Run each scraper and process results
    try:
        # Pracuj.pl scraper
        logging.info("Starting pracuj.pl scraper...")
        jobs, skills = scrape_pracuj()
        for job in jobs:
            job_id = insert_job_listing(job)
            if job_id:
                process_skills(job, skills.get(job.job_id, []), skill_categories)
                total_skills += len(skills.get(job.job_id, []))
        total_jobs += len(jobs)
        logging.info(f"Completed pracuj.pl scraper. Found {len(jobs)} jobs.")
        
        # Add other scrapers here as we build them
        
        # Log results
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info(f"Job scraper completed in {duration:.2f} seconds")
        logging.info(f"Total jobs scraped: {total_jobs}")
        logging.info(f"Total skills extracted: {total_skills}")
        logging.info(f"Errors encountered: {errors}")
        
    except Exception as e:
        logging.error(f"Error in run_scraper: {str(e)}")
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    # For local testing
    run_scraper()
