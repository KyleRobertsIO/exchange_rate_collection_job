from enum import Enum

class PostgresSelectQuery(Enum):

    COUNT_EXCHANGE_RATE_BY_DATE_AND_SOURCE = "SELECT COUNT(date) FROM dbo.exchange_rates WHERE date = :date AND source = :source"

    def __str__(self):
        return self.value
    
class PostgresDMLQuery(Enum):

    INSERT_EXCHANGE_RATE = "INSERT INTO dbo.exchange_rates (date, rates, source) VALUES (:date, :rates, :source)"

    def __str__(self):
        return self.value