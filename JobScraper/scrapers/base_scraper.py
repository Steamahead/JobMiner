from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Set
import logging
import requests
import sys
import os

# Try multiple paths to find BeautifulSoup
try:
    from bs4 import BeautifulSoup
except ImportError:
    # Try to find packages in the Python packages directory
    site_packages = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                               '.python_packages', 'lib', 'site-packages')
    if os.path.exists(site_packages):
        sys.path.append(site_packages)
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            # If bs4 still not found, try with the beautifulsoup4 package
            try:
                from beautifulsoup4 import BeautifulSoup
            except ImportError:
                # Last resort - raise a clear error
                raise ImportError("BeautifulSoup cannot be imported. Please ensure bs4 is correctly installed.")

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
