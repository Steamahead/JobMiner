import datetime
import time
import logging
import os
import azure.functions as func
import traceback

def main(mytimer: func.TimerRequest) -> None:
    start = time.time()
    try:
        # 1) Log trigger timestamp
        current_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        logging.warning(f"🔥 JOB SCRAPER TRIGGERED AT: {current_time.isoformat()} 🔥")

        # 2) Log essential environment variables
        logging.warning(f'DB_SERVER exists: {os.environ.get("DB_SERVER") is not None}')
        logging.warning(f'DB_NAME   exists: {os.environ.get("DB_NAME")   is not None}')
        logging.warning(f'DB_UID    exists: {os.environ.get("DB_UID")    is not None}')
        logging.warning(f'DB_PWD    exists: {os.environ.get("DB_PWD")    is not None}')

        # 3) Import and run the scraper
        from .scraper import run_scraper
        logging.warning("Successfully imported run_scraper")

        run_scraper()
        logging.warning("🔥 Job scraper completed successfully 🔥")

    except ImportError as import_err:
        logging.error(f"Import error: {import_err}")
        logging.error(traceback.format_exc())
        raise

    except Exception as e:
        logging.error(f"Execution error: {e}")
        logging.error(traceback.format_exc())
        raise

    finally:
        # 4) Always log total elapsed time
        elapsed = time.time() - start
        logging.warning(f"🕒 Scrape total duration: {elapsed:.1f} seconds")
