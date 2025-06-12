from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

@dataclass
class JobListing:
    # Core Fields
    job_id: str                  # External ID from job site
    source: str                  # Which job board (e.g., "LinkedIn", "Indeed")
    title: str                   # Job title
    company: str                 # Company name
    link: str                    # URL to the job posting
    salary_min: Optional[int]    # Minimum salary if available
    salary_max: Optional[int]    # Maximum salary if available
    location: str                # Job location
    operating_mode: str          # Remote/Hybrid/On-site
    work_type: str               # Full-time, Part-time
    experience_level: str        # Entry-level, Junior, Mid, Senior
    employment_type: str         # B2B, Contract, etc.
    years_of_experience: Optional[int]  # Required years of experience
    scrape_date: datetime        # When this listing was scraped
    published_date: Optional[datetime] = None  # When the job was originally posted
    listing_status: str          # Active/Expired
    short_id: Optional[int] = None

@dataclass
class Skill:
    # Skills table
    job_id: str                  # Foreign key to JobListing
    source: str                  # Which job board (e.g., "pracuj.pl")
    skill_name: str              # Name of the skill (SQL, Power BI, etc.)
    skill_category: str          # Category (Database, Visualization, etc.)
    short_id: Optional[int] = None
