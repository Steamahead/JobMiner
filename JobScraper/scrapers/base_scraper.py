from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, TYPE_CHECKING
if TYPE_CHECKING:
    from ..models import JobListing

import logging
import random
import time
import requests

from bs4 import BeautifulSoup   # bs4 is already installed in your env

class BaseScraper(ABC):
    """Base class for all job scrapers"""

    def __init__(self):
        # 1) HTTP headers for all requests
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        # 2) Logger scoped to the concrete scraper class
        self.logger = logging.getLogger(self.__class__.__name__)

    # -------------------------------------------------------
    # Utility: robust HTTP GET with retry & back-off
    # -------------------------------------------------------
    def get_page_html(self, url: str, max_retries: int = 3, base_delay: float = 1.0) -> str:
        """Return HTML or empty string if all retries fail."""
        retries = 0
        while retries < max_retries:
            try:
                # Randomised polite delay
                time.sleep(base_delay + random.uniform(0, 1.5))

                resp = requests.get(url, headers=self.headers, timeout=30)
                self.logger.info(f"GET {url} → {resp.status_code}, {len(resp.text)} bytes")

                # Handle rate-limit
                if resp.status_code == 429:
                    wait = base_delay * (2 ** retries) + random.uniform(0, 3)
                    self.logger.warning(
                        f"429 Rate-limited – retrying in {wait:.1f}s (attempt {retries+1}/{max_retries})"
                    )
                    time.sleep(wait)
                    retries += 1
                    continue

                resp.raise_for_status()
                return resp.text

            except Exception as exc:
                retries += 1
                wait = base_delay * (2 ** retries) + random.uniform(0, 3)
                self.logger.error(f"Error fetching {url}: {exc} – retry {retries}/{max_retries}")
                time.sleep(wait)

        self.logger.error(f"Failed to fetch {url} after {max_retries} attempts")
        return ""

    # -------------------------------------------------------
    # Every concrete scraper must implement scrape()
    # -------------------------------------------------------
    @abstractmethod
    def scrape(self) -> Tuple[List["JobListing"], Dict[str, List[str]]]:
        """Return (list_of_job_listings, {job_id: [skills…]})"""
        raise NotImplementedError
