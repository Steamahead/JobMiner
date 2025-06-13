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

import threading, collections, time

_RATE = 4.0              # requests / second across the whole process
_tokens = collections.deque([0.0])   # time stamps
_lock   = threading.Lock()

def _wait_for_slot():
    while True:
        with _lock:
            now = time.perf_counter()
            # purge timestamps older than 1 sec
            while _tokens and now - _tokens[0] > 1.0:
                _tokens.popleft()
            if len(_tokens) < _RATE:
                _tokens.append(now)
                return
        time.sleep(0.05)

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
        self._local = threading.local()
        self.logger = logging.getLogger(self.__class__.__name__)
    @property
    def session(self):
        if not hasattr(self._local, "session"):
            s = requests.Session()
            # Optional one-liner: rotate UA every 30 requests
            s.headers.update({"Accept-Encoding": "gzip"})
            self._local.session = s
        return self._local.session
        # 2) Logger instance scoped to this scraper
            
    def get_page_html(self, url, max_retries=4, base_delay=0.5) -> str:
        retries = 0
        while retries < max_retries:
            try:
                _wait_for_slot()                                # throttle
                time.sleep(random.uniform(0, base_delay))       # per-thread jitter
    
                resp = self.session.get(url, headers=self.headers, timeout=15)
                # treat 429 as retryable
                if resp.status_code == 429:
                    raise ValueError("HTTP 429")
                text = resp.text or ""
    
                # stub-page detection
                if len(text) < 2000 or "nie wspieramy" in text.lower():
                    raise ValueError("stub html")
    
                return text
    
            except Exception as e:
                retries += 1
                backoff = (2 ** retries) * base_delay + random.random()
                self.logger.warning(f"{url} failed: {e} (retry {retries}/{max_retries})")
                time.sleep(backoff)
        self.logger.error(f"Giving up on {url} after {max_retries} attempts")
        return ""
           
    @abstractmethod
    def scrape(self) -> Tuple[List['JobListing'], Dict[str, List[str]]]:
        """
        Main scraping method to be implemented by each specific scraper
        Returns: (job_listings, skills_dict)
        """
        pass
