import json
from abc import ABCMeta
from typing import List

from src.database import SQLDatabase
from src.database.Errors import DatabaseQueryError
from src.database.Queries import PostgresDMLQuery, PostgresSelectQuery
from src.entities.ExchangeRate import ExchangeRate


class IExchangeRateRepo(metaclass = ABCMeta):

    def insert_exchange_rate(entity: ExchangeRate):
        pass

    def if_exists_by_date_and_source() -> int:
        pass

class ExchangeRateRepo(IExchangeRateRepo):
    
    def __init__(self, sql_db: SQLDatabase):
        self._db = sql_db

    def insert_exchange_rate(self, entity: ExchangeRate):
        try:
            self._db.execute(
                query = PostgresDMLQuery.INSERT_EXCHANGE_RATE, 
                args = { "date": entity.date, "source": entity.source, "rates": json.dumps(entity.rates) }
            )
        except Exception as query_err:
            raise DatabaseQueryError(query_err.args)

    def if_exists_by_date_and_source(self, date: str, source: str) -> int:
        try:
            result: List[dict] = self._db.select(
                query = PostgresSelectQuery.COUNT_EXCHANGE_RATE_BY_DATE_AND_SOURCE, 
                args = { "date":  date, "source": source }
            )
            return result[0].get("count")
        except Exception as query_err:
            raise DatabaseQueryError(query_err.args)
