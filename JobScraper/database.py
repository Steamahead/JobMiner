from typing import Optional
import os
import logging
from datetime import datetime
from .models import JobListing, Skill


def _truncate(value: Optional[str], length: int) -> Optional[str]:
    """Helper to ensure string does not exceed the given length."""
    if isinstance(value, str) and len(value) > length:
        return value[:length]
    return value

def get_sql_connection():
    """Get SQL connection using SQL authentication"""
    try:
        # Import pymssql directly
        import pymssql

        # Get connection details from environment variables
        server = os.environ.get('DB_SERVER')
        database = os.environ.get('DB_NAME')
        username = os.environ.get('DB_UID')
        password = os.environ.get('DB_PWD')

        logging.info(f"Connecting to SQL server with SQL auth: {server}/{database} as {username}")

        # Connect using pymssql with SQL authentication
        connection = pymssql.connect(
            server=server,
            user=username,
            password=password,
            database=database,
            timeout=30,
            appname="JobMinerApp"
        )

        logging.info("SQL connection successful with SQL auth")
        return connection
    except Exception as e:
        logging.error(f"SQL connection error: {str(e)}")
        import traceback
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
                PublishedDate DATETIME NULL,
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
        import traceback
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
    cur = conn.cursor()

    # check for existing
    cur.execute(
        "SELECT ID FROM JobListings WHERE JobID=%s AND Source=%s",
        (job.job_id, job.source)
    )
    row = cur.fetchone()
    if row:
        job.short_id = row[0]
        return row[0]

    # insert new
    # enforce column length limits
    params = (
        _truncate(job.job_id, 100),
        _truncate(job.source, 50),
        _truncate(job.title, 255),
        _truncate(job.company, 255),
        _truncate(job.link, 500),
        job.salary_min,
        job.salary_max,
        _truncate(job.location, 255),
        _truncate(job.operating_mode, 50),
        _truncate(job.work_type, 50),
        _truncate(job.experience_level, 50),
        _truncate(job.employment_type, 50),
        job.years_of_experience,
        job.scrape_date,
        _truncate(job.listing_status, 20),
    )

    cur.execute(
        """
        INSERT INTO JobListings (
            JobID, Source, Title, Company, Link,
            SalaryMin, SalaryMax, Location,
            OperatingMode, WorkType, ExperienceLevel, EmploymentType,
            YearsOfExperience, ScrapeDate, ListingStatus
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        SELECT SCOPE_IDENTITY();
    """,
        params,
    )
    new_id = int(cur.fetchone()[0])
    conn.commit()
    job.short_id = new_id
    cur.close()
    conn.close()
    return new_id
def insert_skill(skill: Skill) -> bool:
    """Insert a skill into the database"""
    connection = None
    cursor = None
    
    try:
        connection = get_sql_connection()
        if not connection:
            logging.error(f"Failed to establish database connection for skill: {skill.skill_name}")
            return False
            
        cursor = connection.cursor()
        
        # Check if skill already exists for this job
        check_query = """
        SELECT ID, ShortID
          FROM Skills
         WHERE JobID = %s
           AND Source = %s
           AND SkillName = %s
        """
        cursor.execute(check_query, (skill.job_id, skill.source, skill.skill_name))
        existing = cursor.fetchone()
        
        if existing:
            # write back the existing ShortID
            skill.short_id = existing[1]
            logging.info(f"Skill already exists: {skill.skill_name} for job {skill.job_id}")
            return True
        
        # Insert new skill
        insert_query = """
        INSERT INTO Skills (
            JobID,
            ShortID,
            Source,
            SkillName,
            SkillCategory
        ) VALUES (%s, %s, %s, %s, %s)
        """
        
        params = (
            _truncate(skill.job_id, 100),
            skill.short_id,
            _truncate(skill.source, 50),
            _truncate(skill.skill_name, 100),
            _truncate(skill.skill_category, 50)
        )
        
        cursor.execute(insert_query, params)
        connection.commit()
        
        logging.info(f"Inserted skill: {skill.skill_name} for job {skill.job_id}")
        return True
        
    except Exception as e:
        if connection:
            connection.rollback()
        import traceback
        logging.error(f"Error inserting skill {skill.skill_name}: {e}")
        logging.error(traceback.format_exc())
        return False
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
