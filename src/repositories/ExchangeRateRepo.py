# from src.PostgresDatabase import PostgresConnection
# from src.exchange.entities import ExchangeRateHostRate
# import json
# from datetime import date
# from typing import List
# from psycopg2.extras import execute_values

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
    
    # def insert_rates(entities: List[ExchangeRate]):
    #     pass

    # def merge_exchange_rates():
    #     pass

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

# class ExchangeRateRepo:
#     def __init__(self, conn: PostgresConnection):
#         self._conn = conn
#         self._schemaName = "dbo"
#         self._tableName = "exchange_rates"
    
#     def insertRate(self, entity: ExchangeRateHostRate):
#         cursor = self._conn.getCursor()
#         query: str = f"""
#         INSERT INTO {self._schemaName}.{self._tableName}
#         (date, rates, source)
#         VALUES (%s, %s, %s)
#         """
#         cursor.execute(
#             query = query,
#             vars = (entity.date, json.dumps(entity.rates), entity.source)
#         )
#         self._conn.commit()

#     def _tupleExchangeRateHostRate(self, collection: List[ExchangeRateHostRate]) -> List[tuple]:
#         tupleCollection: List[tuple] = []
#         for e in collection:
#             tupleCollection.append((
#                 e.date,
#                 json.dumps(e.rates),
#                 e.source
#             ))
#         return tupleCollection

#     def mergeExchangeRates(self):
#         cursor = self._conn.getCursor()
#         query: str = "CALL staging.mergeExchangeRates()"
#         cursor.execute(query = query)
#         self._conn.commit()

#     def insertRates(self, collection: List[ExchangeRateHostRate]):
#         cursor = self._conn.getCursor()
#         query: str = f"""
#         INSERT INTO staging.{self._tableName}
#         (date, rates, source)
#         VALUES %s
#         """
#         execute_values(
#             cur = cursor, 
#             sql = query, 
#             argslist = self._tupleExchangeRateHostRate(collection)
#         )
#         self._conn.commit()

#     def ifExistsByDateAndSource(self, date: date, source: str) -> int:
#         cursor = self._conn.getCursor()
#         query: str = f"""
#         SELECT COUNT(date)
#         FROM {self._schemaName}.{self._tableName}
#         WHERE date = %s
#         AND source = %s
#         """
#         cursor.execute(
#             query = query,
#             vars = (date, source)
#         )
#         count: int = cursor.fetchone()[0]
#         return count