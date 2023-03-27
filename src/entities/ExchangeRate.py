from datetime import date
from dataclasses import dataclass

@dataclass
class ExchangeRate:
    date: date
    rates: dict
    source: str