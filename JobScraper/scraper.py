# steamahead/jobminer/JobMiner-04dfa4217749e2412b383a580c273e2fed5cebed/scraper.py
import logging
from datetime import datetime
from typing import List
import traceback

from .database import create_tables_if_not_exist, insert_job_listing, insert_skills_for_job
from .models import JobListing, Skill

# Import individual scrapers
from .scrapers.pracuj_scraper import scrape_pracuj
# from .scrapers.justjoin_scraper import scrape_justjoin

def process_skills(job_listing: JobListing, skills_list: List[str], skill_categories: dict) -> List[Skill]:
    """
    Creates a list of Skill objects for a given job.
    """
    processed_skills = []
    for skill_name in skills_list:
        category = "Other"
        for cat, skills_in_cat in skill_categories.items():
            if skill_name.lower() in skills_in_cat:
                category = cat
                break
        
        skill = Skill(
            job_id=job_listing.job_id,
            source=job_listing.source,
            skill_name=skill_name,
            skill_category=category,
            short_id=job_listing.short_id
        )
        processed_skills.append(skill)
    return processed_skills

def run_scraper():
    """Main function to coordinate all scraping activities"""
    start_time = datetime.now()
    logging.info(f"Starting job scraper at {start_time}")
    
    from .scrapers.pracuj_scraper import PracujScraper
    temp_scraper = PracujScraper()
    skill_categories = temp_scraper.skill_categories
    
    if not create_tables_if_not_exist():
        logging.error("Failed to create database tables. Exiting.")
        return
    
    total_jobs = 0
    total_skills = 0
    
    try:
        logging.info("Starting pracuj.pl scraper...")
        jobs, skills_dict = scrape_pracuj()
        
        for job in jobs:
            # Insert the job and get its database ID
            job.short_id = insert_job_listing(job)
            if not job.short_id:
                logging.error(f"Failed to insert job {job.job_id}; skipping its skills")
                continue
            
            # Get the list of skills for this job
            job_skills_list = skills_dict.get(job.job_id, [])
            if job_skills_list:
                # Prepare Skill objects
                skill_objects = process_skills(job, job_skills_list, skill_categories)
                # Bulk insert all skills for this job
                insert_skills_for_job(job, skill_objects)
                total_skills += len(skill_objects)

        total_jobs += len(jobs)
        logging.info(f"Completed pracuj.pl scraper. Found {len(jobs)} jobs.")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info(f"Job scraper completed in {duration:.2f} seconds")
        logging.info(f"Total jobs scraped: {total_jobs}")
        logging.info(f"Total skills processed: {total_skills}")
        
    except Exception as e:
        logging.error(f"Error in run_scraper: {str(e)}", exc_info=True)

if __name__ == "__main__":
    run_scraper()
