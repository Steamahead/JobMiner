import datetime
import logging
import os
import azure.functions as func
from .scraper import run_scraper

def main(mytimer: func.TimerRequest) -> None:
    try:
        # Get current time
        current_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        logging.warning(f"🔥 JOB SCRAPER TRIGGERED AT: {current_time.isoformat()} 🔥")
        
        # Log environment variables (omit password)
        logging.warning(f'DB_SERVER: {os.environ.get("DB_SERVER")}')
        logging.warning(f'DB_NAME: {os.environ.get("DB_NAME")}')
        logging.warning(f'DB_UID: {os.environ.get("DB_UID")}')
        logging.warning(f'DB_PWD exists: {os.environ.get("DB_PWD") is not None}')
        
        # Import diagnostic
        try:
            from .scraper import run_scraper
            logging.warning("Successfully imported run_scraper")
        except Exception as import_err:
            logging.error(f"Import error: {str(import_err)}")
            raise
            
        # Run the scraper with more error handling
        try:
            run_scraper()
            logging.warning('🔥 Job scraper completed successfully 🔥')
        except Exception as run_err:
            logging.error(f"Scraper execution error: {str(run_err)}")
            import traceback
            logging.error(traceback.format_exc())
            raise
            
    except Exception as e:
        logging.error(f'Top-level error in job scraper: {str(e)}')
        import traceback
        logging.error(traceback.format_exc())
        raise
