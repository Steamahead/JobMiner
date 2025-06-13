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
        """Initialize common request headers and a logger."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) "
                "Gecko/20100101 Firefox/117.0"
            )
        }

    def get_page_html(self, url: str, max_retries=3, base_delay=1) -> str:
        """Get HTML content from a URL with retry logic and random delays"""
        retries = 0
        while retries < max_retries:
            try:
                # random delay between requests
                delay = base_delay + random.uniform(0, 1.5)
                time.sleep(delay)

                response = requests.get(url, headers=self.headers, timeout=30)
                self.logger.info(f"GET {url} → {response.status_code}, {len(response.text)} bytes")

                # handle rate-limit
                if response.status_code == 429:
                    retry_delay = base_delay * (2 ** retries) + random.uniform(0, 3)
                    self.logger.warning(
                        f"Rate limited, retrying in {retry_delay:.1f}s "
                        f"(attempt {retries+1}/{max_retries})"
                    )
                    time.sleep(retry_delay)
                    retries += 1
                    continue

                response.raise_for_status()
                return response.text

            except Exception as e:
                retries += 1
                retry_delay = base_delay * (2 ** retries) + random.uniform(0, 3)
                self.logger.error(f"Error fetching {url}: {e} (retry {retries}/{max_retries})")
                time.sleep(retry_delay)

        # all retries failed
        self.logger.error(f"Failed to fetch {url} after {max_retries} attempts")
        return ""

    @abstractmethod
    def scrape(self) -> Tuple[List['JobListing'], Dict[str, List[str]]]:
        """
        Main scraping method to be implemented by each specific scraper
        Returns: (job_listings, skills_dict)
        """
        pass
