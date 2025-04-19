from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Set
import logging
import requests
from bs4 import BeautifulSoup

class BaseScraper(ABC):
    """Base class for all job scrapers"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
            'Accept-Language': 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
    
    def get_page_html(self, url: str) -> str:
        """Get HTML content from a URL"""
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logging.error(f"Error fetching URL {url}: {str(e)}")
            return ""
            
    @abstractmethod
    def scrape(self) -> Tuple[List, Dict]:
        """
        Main scraping method to be implemented by each specific scraper
        Returns: (job_listings, skills_dict)
        """
        pass
