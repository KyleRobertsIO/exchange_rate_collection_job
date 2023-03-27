import sys
from logging import Logger
from datetime import datetime, timedelta
from typing import List

from src.entities.ExchangeRate import ExchangeRate
from src.repositories.ExchangeRateRepo import ExchangeRateRepo
from src.clients.ExchangeRateHost import (
    ExchangeRateHostProxy, 
    BaseCurrency,
    DatedRates
)

class RatesDuplicationError(Exception):
    def __init__(self, date: datetime, source: str):
        '''
        Raised when there is a duplicate record found inside of the database table.
        '''
        super().__init__(
            self, 
            "Duplicate record was found inside of database table.",
            date.strftime("%Y-%m-%d"),
            source
        )

class HistoricalReateLoaderService:

    def __init__(
        self, logger: Logger, repo: ExchangeRateRepo
    ):
        self._logger = logger
        self._repo = repo

    def _create_client(self) -> ExchangeRateHostProxy:
        client = ExchangeRateHostProxy(
            base_currency = BaseCurrency.USD, 
            logger = self._logger
        )
        return client

    def _cast_entity_collection(
        self, collection: List[DatedRates]
    ) -> List[ExchangeRate]:
        return [
            ExchangeRate(
                date = x.date, rates = x.rates, source = "EXCHANGE_RATE_HOST"
            ) for x in collection
        ]

    def _insert_record(self, entity: ExchangeRate):
        try:
            self._repo.insert_exchange_rate(entity = entity)
        except Exception as err:
            raise err
    
    def _check_for_duplicates(self, date: datetime, source: str):
        total: int = self._repo.if_exists_by_date_and_source(
            date = date.strftime("%Y-%m-%d"), source = source
        )
        if total > 0:
            raise RatesDuplicationError(date = date, source = source)

    def _save_rate_history(self, collection: List[ExchangeRate]):
        for record in collection:
            try:
                self._check_for_duplicates(
                    date = record.date, source = record.source
                )
            except RatesDuplicationError as dup_err:
                self._logger.warning(str(dup_err.args))
                continue
            try:
                self._insert_record(entity = record)
            except Exception as save_err:
                self._logger.error("Failed to save record dated {0}. {1}".format(
                    record.date.strftime("%Y-%m-%d"), save_err.args
                ))
                sys.exit()

    def _collect_historical_rates(
        self, 
        client: ExchangeRateHostProxy, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[DatedRates]:
        try:
            rates_collection: List[DatedRates] = client.get_rates_for_date_range(
                start_date = start_date,
                end_date = end_date
            )
            return rates_collection
        except Exception as collection_err:
            self._logger.error("Failed to collect date from ExchangeRateHost. {0}".format(
                collection_err.args
            ))
            sys.exit()

    def load(self, end_date: datetime, previous_days: int):
        time_delta = timedelta(days = previous_days)
        start_date: datetime = end_date - time_delta
        client = self._create_client()
        rates_collection: List[DatedRates] = self._collect_historical_rates(
            client = client,
            start_date = start_date,
            end_date = end_date
        )
        entities: List[ExchangeRate] = self._cast_entity_collection(
            collection = rates_collection
        )
        self._save_rate_history(collection = entities)