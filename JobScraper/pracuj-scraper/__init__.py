import azure.functions as func
from pracuj_scraper import PracujScraper

def main(timer: func.TimerRequest):
    scraper = PracujScraper()
    scraper.scrape()
