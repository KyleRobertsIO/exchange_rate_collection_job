from dotenv import load_dotenv
from logging import Logger
from datetime import datetime, timedelta

from src.config.Environment import get_environment_config
from src.database import PostgresDatabase, SQLConnectionDetails

from src.enums import JobType
from src.repositories.ExchangeRateRepo import ExchangeRateRepo

from src.services.NightlyRateCollectorService import NightlyRateCollectorService
from src.services.HistoricalRateLoaderService import HistoricalReateLoaderService

# Logging
from src.logger import (
    build_json_logging_handler, 
    build_stdout_logging_handler
)

load_dotenv()
env_config: dict = get_environment_config()
    
logger = Logger(name = env_config["logger"]["name"])
logger.setLevel(env_config["logger"]["log_level"])
logger.addHandler(build_stdout_logging_handler())
logger.addHandler(build_json_logging_handler(
    file_name = env_config["logger"]["json_file_path"]
))

sql_details = SQLConnectionDetails(
    host = env_config["postgres"]["host"],
    port = env_config["postgres"]["port"],
    database = env_config["postgres"]["database"],
    username = env_config["postgres"]["username"],
    password = env_config["postgres"]["password"],
)
postgres = PostgresDatabase(connection_details = sql_details)

exchange_rate_repo = ExchangeRateRepo(sql_db = postgres)

def run_nightly_data_collection():
    logger.info("Starting Nightly Exchange Rate Collection Job")
    curr_timestamp = datetime.now()
    yesterday_timestamp = curr_timestamp - timedelta(days = 1)
    nightly_collector = NightlyRateCollectorService(
        repo = exchange_rate_repo, logger = logger
    )
    nightly_collector.save_rate(target_date = yesterday_timestamp)
    logger.info("Completed Nightly Exchange Rate Collection Job")

def run_historical_data_collection():
    logger.info("Starting Historical Exchange Rate Collection Job")
    historical_collector = HistoricalReateLoaderService(
        logger = logger, repo = exchange_rate_repo
    )
    historical_collector.load(
        end_date = env_config["job"]["historical_end_date"],
        previous_days = env_config["job"]["historical_previous_days"]
    )
    logger.info("Completed Historical Exchange Rate Collection Job")

if __name__ == "__main__":
    match env_config["job"]["type"]:
        case JobType.NIGHTLY:
            run_nightly_data_collection()
        case JobType.HISTORICAL:
            run_historical_data_collection()

