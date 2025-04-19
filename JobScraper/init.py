import datetime
import logging
import os
import azure.functions as func
from .scraper import run_scraper

def main(mytimer: func.TimerRequest) -> None:
    # Get current time
    current_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    logging.warning(f"🔥 JOB SCRAPER TRIGGERED AT: {current_time.isoformat()} 🔥")
    
    if mytimer.past_due:
        logging.warning('🔥 The timer is past due! 🔥')

    try:
        logging.warning('🔥 Starting job scraper... 🔥')
        logging.info(f'DB_SERVER exists: {os.environ.get("DB_SERVER") is not None}')
        logging.info(f'DB_NAME exists: {os.environ.get("DB_NAME") is not None}')
        logging.info(f'DB_UID exists: {os.environ.get("DB_UID") is not None}')
        logging.info(f'DB_PWD exists: {os.environ.get("DB_PWD") is not None}')
        
        # Run the scraper
        run_scraper()
        logging.warning('🔥 Job scraper completed successfully 🔥')
    except Exception as e:
        logging.error(f'Error in job scraper: {str(e)}')
        import traceback
        logging.error(traceback.format_exc())
