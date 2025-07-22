# steamahead/jobminer/JobMiner-04dfa4217749e2412b383a580c273e2fed5cebed/database.py
from typing import Optional, List
import os
import logging
from datetime import datetime
from .models import JobListing, Skill
import pymssql
import traceback


def _truncate(value: Optional[str], length: int) -> Optional[str]:
    """Helper to ensure string does not exceed the given length."""
    if isinstance(value, str) and len(value) > length:
        return value[:length]
    return value

def get_sql_connection():
    """Get SQL connection using SQL authentication"""
    try:
        server = os.environ.get('DB_SERVER')
        database = os.environ.get('DB_NAME')
        username = os.environ.get('DB_UID')
        password = os.environ.get('DB_PWD')

        connection = pymssql.connect(
            server=server,
            user=username,
            password=password,
            database=database,
            timeout=30,
            appname="JobMinerApp"
        )
        return connection
    except Exception as e:
        logging.error(f"SQL connection error: {str(e)}")
        logging.error(traceback.format_exc())
        return None

def create_tables_if_not_exist():
    """Create the database tables if they don't exist"""
    connection = None
    try:
        connection = get_sql_connection()
        if not connection:
            logging.error("Failed to establish database connection")
            return False
            
        cursor = connection.cursor()
        
        # Create Jobs table
        jobs_table_sql = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'JobListings')
        BEGIN
            CREATE TABLE JobListings (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                JobID NVARCHAR(100) NOT NULL,
                Source NVARCHAR(50) NOT NULL,
                Title NVARCHAR(255) NOT NULL,
                Company NVARCHAR(255) NOT NULL,
                Link NVARCHAR(500) NOT NULL,
                SalaryMin INT NULL,
                SalaryMax INT NULL,
                Location NVARCHAR(255) NOT NULL,
                OperatingMode NVARCHAR(50) NOT NULL,
                WorkType NVARCHAR(50) NOT NULL,
                ExperienceLevel NVARCHAR(50) NOT NULL,
                EmploymentType NVARCHAR(50) NOT NULL,
                YearsOfExperience INT NULL,
                ScrapeDate DATETIME NOT NULL,
                ListingStatus NVARCHAR(20) NOT NULL,
                CONSTRAINT UC_JobListing UNIQUE (JobID, Source)
            )
        END
        """
        
        # Create Skills table
        skills_table_sql = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Skills')
        BEGIN
            CREATE TABLE Skills (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                JobID NVARCHAR(100) NOT NULL,
                ShortID INT NOT NULL,
                Source NVARCHAR(50) NOT NULL,
                SkillName NVARCHAR(100) NOT NULL,
                SkillCategory NVARCHAR(50) NOT NULL,
                CONSTRAINT UC_JobSkill UNIQUE (JobID, Source, SkillName)
            )
        END
        """
        
        cursor.execute(jobs_table_sql)
        cursor.execute(skills_table_sql)
        connection.commit()
        logging.info("Database tables created or already exist")
        return True
        
    except Exception as e:
        logging.error(f"Error creating tables: {str(e)}")
        logging.error(traceback.format_exc())
        return False
    finally:
        if connection:
            connection.close()

def insert_job_listing(job: JobListing) -> Optional[int]:
    """Insert a job listing into the database and return its ID"""
    conn = get_sql_connection()
    if not conn:
        logging.error(f"DB connect failed for job {job.title}")
        return None
    
    try:
        with conn.cursor() as cur:
            # Check for existing job
            cur.execute(
                "SELECT ID FROM JobListings WHERE JobID=%s AND Source=%s",
                (job.job_id, job.source)
            )
            row = cur.fetchone()
            if row:
                job.short_id = row[0]
                return row[0]

            # Insert new job if not found
            params = (
                _truncate(job.job_id, 100), _truncate(job.source, 50),
                _truncate(job.title, 255), _truncate(job.company, 255),
                _truncate(job.link, 500), job.salary_min, job.salary_max,
                _truncate(job.location, 255), _truncate(job.operating_mode, 50),
                _truncate(job.work_type, 50), _truncate(job.experience_level, 50),
                _truncate(job.employment_type, 50), job.years_of_experience,
                job.scrape_date, _truncate(job.listing_status, 20),
            )

            cur.execute(
                """
                INSERT INTO JobListings (
                    JobID, Source, Title, Company, Link,
                    SalaryMin, SalaryMax, Location,
                    OperatingMode, WorkType, ExperienceLevel, EmploymentType,
                    YearsOfExperience, ScrapeDate, ListingStatus
                ) OUTPUT INSERTED.ID
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """,
                params,
            )
            new_id = int(cur.fetchone()[0])
            conn.commit()
            job.short_id = new_id
            return new_id
    except Exception as e:
        logging.error(f"Error inserting job {job.job_id}: {e}", exc_info=True)
        conn.rollback()
        return None
    finally:
        if conn:
            conn.close()

def insert_skills_for_job(job: JobListing, skills: List[Skill]):
    """Insert a list of skills for a given job in a single transaction."""
    if not skills or not job.short_id:
        return

    conn = get_sql_connection()
    if not conn:
        logging.error(f"DB connect failed for skills of job {job.job_id}")
        return

    try:
        with conn.cursor() as cur:
            # Prepare the parameters for all skills
            params_list = []
            for skill in skills:
                params_list.append((
                    _truncate(job.job_id, 100),
                    job.short_id,
                    _truncate(job.source, 50),
                    _truncate(skill.skill_name, 100),
                    _truncate(skill.skill_category, 50)
                ))

            # Use executemany for efficient bulk insert
            # IGNORE_DUP_KEY handles cases where a skill already exists, preventing errors
            insert_query = """
            INSERT INTO Skills (JobID, ShortID, Source, SkillName, SkillCategory)
            VALUES (%s, %s, %s, %s, %s)
            """
            
            # Since pymssql's executemany doesn't support IGNORE_DUP_KEY, we'll do it manually.
            # This is less efficient than a true bulk insert but still much better than single inserts.
            for params in params_list:
                try:
                    cur.execute(insert_query, params)
                except pymssql.IntegrityError:
                    # This happens if the skill already exists due to the UNIQUE constraint. We can safely ignore it.
                    logging.info(f"Skill '{params[3]}' already exists for job {params[0]}. Skipping.")
                    conn.rollback() # Rollback the failed insert before continuing
                except Exception as ex:
                    logging.error(f"Error inserting skill '{params[3]}': {ex}")
                    conn.rollback()


            conn.commit()
            logging.info(f"Processed {len(skills)} skills for job {job.job_id}")

    except Exception as e:
        logging.error(f"Error bulk inserting skills for job {job.job_id}: {e}", exc_info=True)
        conn.rollback()
    finally:
        if conn:
            conn.close()
