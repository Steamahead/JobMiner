JobMiner - pracuj.pl Data Analyst Scraper

JobMiner is an automated Azure Function that scrapes data analyst job listings from pracuj.pl. It extracts detailed information, identifies key skills, and stores the data in a centralized Azure SQL Database.

This scraper is a key component of a larger data analysis portfolio. The database it populates is shared with another scraper (for theprotocol.it), and the combined dataset is used to power interactive Power BI dashboards that visualize trends in the job market for data professionals.

Features

Automated Daily Scraping: Runs on a schedule using an Azure Timer Trigger.
Rich Data Extraction: Captures job titles, companies, salary ranges, locations, and experience levels.
Advanced Skill Parsing: Identifies and categorizes dozens of technical skills from job descriptions.
Efficient & Resilient: Uses a checkpoint system to handle interruptions and prevent duplicate entries.

Technical Stack

Language: Python 3.11
Web Scraping: BeautifulSoup4, requests
Database: Azure SQL Database (pymssql)
Hosting: Azure Functions
CI/CD: GitHub Actions
Visualization: Power BI
