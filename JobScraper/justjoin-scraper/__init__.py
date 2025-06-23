import azure.functions as func
from justjoin_scraper import JustJoinScraper

def main(timer: func.TimerRequest):
    scraper = JustJoinScraper()
    scraper.scrape()
