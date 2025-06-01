from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Set, TYPE_CHECKING
if TYPE_CHECKING:
    from ..models import JobListing
import logging
import requests
import sys
import os
import time
import random
from requests.exceptions import RequestException

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
        # 1) HTTP headers for all requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/98.0.4758.102 Safari/537.36',
            'Accept-Language': 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept': 'text/html,application/xhtml+xml,application/xml;'
                      'q=0.9,*/*;q=0.8'
        }

        # 2) Logger instance scoped to this scraper
        self.logger = logging.getLogger(self.__class__.__name__)

    
    def get_page_html(self, url: str, max_retries=3, base_delay=1) -> str:
        """Get HTML content from a URL with retry logic and random delays"""
        retries = 0
        while retries < max_retries:
            try:
                # Add a random delay between requests (between base_delay and base_delay*2 seconds)

                delay = base_delay + random.uniform(0, 1.5)
                time.sleep(delay)
                            
                response = requests.get(url, headers=self.headers, timeout=30)
                
                # If we hit a rate limit, wait longer and retry
                if response.status_code == 429:
                    retry_delay = base_delay * (2 ** retries) + random.uniform(0, 3)
                    logging.warning(f"Rate limited, waiting {retry_delay:.2f} seconds before retry {retries+1}/{max_retries}")
                    time.sleep(retry_delay)
                    retries += 1
                    continue
                    
                response.raise_for_status()
                return response.text
                
            except Exception as e:
                retries += 1
                retry_delay = base_delay * (2 ** retries) + random.uniform(0, 3)     
                logging.error(f"Error fetching URL {url}: {str(e)}")
                logging.info(f"Retrying in {retry_delay:.2f} seconds (attempt {retries}/{max_retries})")
                time.sleep(retry_delay)
        
        return ""  # Return empty string if all retries fail
           
    @abstractmethod
    def scrape(self) -> Tuple[List['JobListing'], Dict[str, List[str]]]:
        """
        Main scraping method to be implemented by each specific scraper
        Returns: (job_listings, skills_dict)
        """
        pass
