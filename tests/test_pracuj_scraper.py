import os
import sys
import pytest
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from JobScraper.scrapers.pracuj_scraper import PracujScraper
from JobScraper.models import JobListing

SAMPLE_DETAIL_HTML = """
<html>
  <body>
    <h1 data-test="text-positionName">Data Analyst</h1>
    <h2 data-test="text-employerName">ACME Corp</h2>
    <ul data-test="sections-benefit-list">
      <li data-test="sections-benefit-workplaces">
        <div data-test="offer-badge-title">Warsaw</div>
      </li>
      <li data-test="sections-benefit-contracts">
        <div data-test="offer-badge-title">B2B</div>
      </li>
    </ul>
    <div data-test="section-salary">
      <div data-test="text-earningAmount">10 000–12 000 zł</div>
    </div>
    <ul data-test="aggregate-open-dictionary-model">
      <li class="catru5k">Python</li>
      <li class="catru5k">SQL</li>
    </ul>
  </body>
</html>
"""

def test_pracujscraper_scrape_returns_joblistings(monkeypatch):
    scraper = PracujScraper()
    scraper.EXPECTED_PER_PAGE = 1

    call = {"count": 0}
    def fake_parse_listings(html):
        if call["count"] == 0:
            call["count"] += 1
            return [{"url": "http://example.com/detail1", "job_id": "job1"}]
        return []
    monkeypatch.setattr(scraper, "_parse_listings", fake_parse_listings)

    class DummyResp:
        def __init__(self, text=""):
            self.text = text
    def dummy_get(url, timeout=30):
        return DummyResp("<html></html>")
    monkeypatch.setattr(scraper.session, "get", dummy_get)

    def fake_get_page_html(url, max_retries=3, base_delay=1):
        return SAMPLE_DETAIL_HTML
    monkeypatch.setattr(scraper, "get_page_html", fake_get_page_html)

    jobs, skills = scraper.scrape()

    assert isinstance(jobs, list)
    assert len(jobs) == 1
    assert isinstance(jobs[0], JobListing)
    assert set(skills[jobs[0].job_id]) == {"python", "sql"}


def test_extract_skills_from_listing_basic():
    html = """
    <ul data-test="aggregate-open-dictionary-model">
      <li class="catru5k">Excel</li>
      <li class="catru5k">Power BI</li>
    </ul>
    """
    soup = BeautifulSoup(html, "html.parser")
    scraper = PracujScraper()
    skills = scraper._extract_skills_from_listing(soup)
    assert set(skills) == {"excel", "power bi"}
