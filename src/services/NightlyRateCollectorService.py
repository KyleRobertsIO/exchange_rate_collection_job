from datetime import datetime, date
from src.clients.ExchangeRateHost import (
    ExchangeRateHostProxy, 
    BaseCurrency, 
    DatedRates
)
from logging import Logger
from src.entities.ExchangeRate import ExchangeRate
from typing import Callable, Any
from src.repositories.ExchangeRateRepo import ExchangeRateRepo

class RatesClientCollectionError(Exception):
    def __init__(self, client: Any, func: Callable):
        '''
        Raised when provided client is unable to collect rates from source system.
        '''
        clientName: str = client.__class__.__name__
        funcName: str = func.__name__
        super().__init__(self, f"Failed to collect rates from client '{clientName}.{funcName}'")

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

class NightlyRateCollectorService:

    def __init__(
        self, repo: ExchangeRateRepo, logger: Logger
    ):
        self._repo = repo
        self._logger = logger

    def _handle_missing_rates(
        self, rates: DatedRates, client: ExchangeRateHostProxy
    ):
        if rates == None:
            raise RatesClientCollectionError(
                client = client,
                func = client.get_rate_for_date
            )

    def _create_client(self) -> ExchangeRateHostProxy:
        return ExchangeRateHostProxy(
            logger = self._logger,
            base_currency = BaseCurrency.USD 
        )

    def _insert_record(self, entity: ExchangeRate):
        try:
            self._repo.insert_exchange_rate(entity = entity)
        except Exception as err:
            raise err

    def _check_for_duplicates(self, date: date, source: str):
        total: int = self._repo.if_exists_by_date_and_source(
            date = date.strftime("%Y-%m-%d"), source = source
        )
        if total > 0:
            raise RatesDuplicationError(date = date, source = source)

    def save_rate(self, target_date: datetime):
        source: str = "EXCHANGE_RATE_HOST"
        self._check_for_duplicates(date = target_date, source = source)
        client = self._create_client()
        dated_rates: DatedRates = client.get_rate_for_date(date = target_date)
        self._handle_missing_rates(rates = dated_rates, client = client)
        entity = ExchangeRate(
            date = dated_rates.date,
            rates = dated_rates.rates,
            source = source
        )
        self._insert_record(entity = entity)