import datetime
import logging
import os
import azure.functions as func
import traceback

def main(mytimer: func.TimerRequest) -> None:
    try:
        # Get current time
        current_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        logging.warning(f"🔥 JOB SCRAPER TRIGGERED AT: {current_time.isoformat()} 🔥")
        
        # Log environment variables
        logging.warning(f'DB_SERVER exists: {os.environ.get("DB_SERVER") is not None}')
        logging.warning(f'DB_NAME exists: {os.environ.get("DB_NAME") is not None}')
        logging.warning(f'DB_UID exists: {os.environ.get("DB_UID") is not None}')
        logging.warning(f'DB_PWD exists: {os.environ.get("DB_PWD") is not None}')
        
        try:
            # Import locally to catch any import errors
            from .scraper import run_scraper
            logging.warning("Successfully imported run_scraper")
            
            # Run the scraper
            run_scraper()
            logging.warning('🔥 Job scraper completed successfully 🔥')
        except ImportError as import_err:
            logging.error(f"Import error: {str(import_err)}")
            logging.error(traceback.format_exc())
            raise
        except Exception as run_err:
            logging.error(f"Execution error: {str(run_err)}")
            logging.error(traceback.format_exc())
            raise
            
    except Exception as e:
        logging.error(f'Top-level error: {str(e)}')
        logging.error(traceback.format_exc())
        raise
