JobMiner is an automated Azure-based application that scrapes data analyst job listings from popular job portals, extracts detailed information including skills, salary ranges, and experience requirements, and stores the structured data in a SQL database. This data is later visualized in Power BI dashboards to help identify in-demand skills and career transition paths.

Features

Automated Data Collection: Daily scheduled scraping of data analyst job listings
Multi-Source Support: Currently implemented for pracuj.pl, with architecture to add more sources
Rich Data Extraction: Captures detailed information including:

Job title, company, and location
Salary ranges (with standardized conversions from hourly to monthly)
Work type and mode (remote/hybrid/on-site)
Experience requirements
Employment types

Skill Detection: Automatically extracts and categorizes skills from job descriptions
Resume Compatibility: Identifies which skills are most in-demand for transitioning professionals
Deduplication: Prevents storing duplicate job listings
Resilience: Checkpoint system for handling interruptions and rate limiting
Structured Storage: All data saved in well-designed SQL tables for easy querying

Architecture
JobMiner is built as an Azure Function App with a timer trigger that runs daily. The application:

Connects to job boards and searches for data analyst positions
Parses HTML using BeautifulSoup4
Extracts detailed information from job listings
Identifies technical skills using pattern matching
Stores data in Azure SQL Database
Provides Power BI integration for visualization

Database Schema
The application uses two main tables:
JobListings Table
Stores core information about each job posting:

ID (auto-incremented)
JobID (external ID from source)
Source (job board name)
Title, Company, Link
Salary range (min/max)
Location, Operating Mode, Work Type
Experience Level, Employment Type, Years of Experience
Scrape Date and Status

Skills Table
Stores skills associated with each job:

ID (auto-incremented)
JobID (foreign key to JobListings)
Source (job board name)
SkillName (standardized skill name)
SkillCategory (Database, Programming, Visualization, etc.)

Technical Stack

Language: Python 3.11
Web Scraping: BeautifulSoup4, Requests
Database: Azure SQL Database (using pymssql)
Hosting: Azure Functions
CI/CD: GitHub Actions
Visualization: Power BI

Deployment
The application is deployed using GitHub Actions with a CI/CD pipeline that:

Builds the Python application
Creates the necessary Python package structure
Uploads the package as an artifact
Deploys to Azure Functions

Scraper Design
JobMiner uses a modular scraper design:

BaseScraper: Abstract base class with common scraping functionality
Source-specific scrapers (like PracujScraper): Handle the specifics of each job board
Skill extraction system with standardized categories and variations

Skills Categories
The system identifies and categorizes skills across multiple domains:

Database: SQL, MySQL, PostgreSQL, etc.
Microsoft BI & Excel: Excel, Power Query, Power Pivot, etc.
Visualization: Power BI, Tableau, Qlik, etc.
Programming: Python, R, Java, etc.
Data Processing: ETL, Spark, Hadoop, etc.
Analytics & Statistics: Regression, Forecasting, etc.
Cloud: AWS, Azure, GCP, etc.
And many more categories

Sample Visualizations
The Power BI dashboard includes visualizations such as:

Most in-demand skills for data analyst positions
Salary ranges by experience level
Job distribution by work mode
Top companies hiring data analysts

License
This project is licensed under the MIT License - see the LICENSE file for details.
