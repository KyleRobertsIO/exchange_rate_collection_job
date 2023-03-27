from dotenv import load_dotenv
from logging import Logger
from datetime import datetime, timedelta

from src.config.Environment import get_environment_config
from src.database import PostgresDatabase, SQLConnectionDetails

from src.repositories.ExchangeRateRepo import ExchangeRateRepo

from src.services.NightlyRateCollectorService import NightlyRateCollectorService

load_dotenv()
env_config: dict = get_environment_config()
    
logger = Logger(name = env_config["logger"]["name"])
logger.setLevel(env_config["logger"]["log_level"])

sql_details = SQLConnectionDetails(
    host = env_config["postgres"]["host"],
    port = env_config["postgres"]["port"],
    database = env_config["postgres"]["database"],
    username = env_config["postgres"]["username"],
    password = env_config["postgres"]["password"],
)
postgres = PostgresDatabase(connection_details = sql_details)

exchange_rate_repo = ExchangeRateRepo(sql_db = postgres)

if __name__ == "__main__":
    logger.info("Starting Exchange Rate Job")
    curr_timestamp = datetime.now()
    yesterday_timestamp = curr_timestamp - timedelta(days = 1)
    nightly_collector = NightlyRateCollectorService(
        repo = exchange_rate_repo, logger = logger
    )
    nightly_collector.save_rate(target_date = yesterday_timestamp)
    logger.info("Completed Exchange Rate Job")
