import requests
import time
from enum import Enum
from datetime import datetime, date
from abc import ABC, abstractmethod
from logging import Logger
from typing import Optional, List
from dataclasses import dataclass

class BaseCurrency(Enum):
    '''
    Availiable currency types to use as a base for `ExchangeRateHost` base currency.
    '''
    USD = "USD"
    CAD = "CAD"
    JPY = "JPY"
    GBP = "GBP"

@dataclass
class DatedRates:
    date: date
    rates: dict

class IExchangeRateHost(ABC):

    def __init__(self, base_currency: BaseCurrency):
        pass

    @abstractmethod
    def get_rate_for_date(self, date: datetime) -> DatedRates:
        pass

    def get_rates_for_date_range(
        self, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[DatedRates]:
        pass

class ExchangeRateHost:
    '''
    API for collecting currency rates from `api.exchangerate.host`.
    '''
    def __init__(
        self, 
        base_currency: BaseCurrency, 
        is_secure: bool = True,
        timeout: int = 10,
        retry_wait: float = 5.0
    ):
        self._domain: str = "api.exchangerate.host"
        self._base_currency: BaseCurrency = base_currency
        self._timeout = timeout
        self._retry_wait = retry_wait
        if is_secure:
            self._protcol = "https"
        else:
            self._protcol = "http"
    
    def _create_dated_rates_list(self, rates_obj: dict) -> List[DatedRates]:
        '''
        From the /timeseries endpoint, the `rates` field is converted to a properly
        formated object for processing.
        '''
        collection: List[DatedRates] = []
        for date_str in rates_obj.keys():
            rates: dict = rates_obj.get(date_str)
            date_obj: date = datetime.strptime(date_str, '%Y-%m-%d').date()
            obj = DatedRates(date = date_obj, rates = rates)
            collection.append(obj)
        return collection

    def _request_execute(self, uri: str, params: dict) -> dict:
        retries: int = 1
        url: str = f'{self._protcol}://{self._domain}{uri}'
        while(retries >= 0):
            try:
                res: requests.Response = requests.get(
                    url = url,
                    params = params,
                    timeout = self._timeout
                )
                retries = -1
            except requests.ConnectTimeout:
                retries = retries - 1
                time.sleep(secs = self._retry_wait)
        payload: dict = res.json()
        rates: dict = payload.get('rates')
        return rates

    def get_rate_for_date(self, date: datetime) -> DatedRates:
        target_date: str = date.strftime("%Y-%m-%d")
        params: dict = { 'base': self._base_currency.value }
        uri: str = f'/{target_date}'
        rates: dict = self._request_execute(uri = uri, params = params)
        obj = DatedRates(date = date.date(), rates = rates)
        return obj

    def get_rates_for_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> List[DatedRates]:
        start_target_date: str = start_date.strftime("%Y-%m-%d")
        end_target_date: str = end_date.strftime("%Y-%m-%d")
        params: dict = {
            'start_date': start_target_date,
            'end_date': end_target_date,
            'base': self._base_currency.value
        }
        uri: str = '/timeseries'
        rates_raw: dict = self._request_execute(uri = uri, params = params)
        rates: List[DatedRates] = self._create_dated_rates_list(rates_obj = rates_raw)
        return rates

class ExchangeRateHostProxy(IExchangeRateHost):

    def __init__(
        self, 
        logger: Logger, 
        base_currency: BaseCurrency = None, 
        client: ExchangeRateHost = None
    ):
        self._logger = logger
        if client != None:
            self._client = client
        else:
            self._client = ExchangeRateHost(base_currency = base_currency)
    
    def _log_timeout_error(self, err: requests.Timeout):
        self._logger.error(f"Connection timeout to {self._client._domain}. {err.args}")

    def _log_connection_error(self, err: requests.ConnectionError):
        self._logger.error(f"Failed to connect to {self._client._domain} for currency convertion rates. {err.args}")

    def get_rate_for_date(self, date: datetime) -> DatedRates:
        rates: Optional[DatedRates] = None
        try:
            rates = self._client.get_rate_for_date(date = date)
        except requests.ConnectionError as conn_err:
            self._log_connection_error(conn_err)
        except requests.Timeout as timeout_err:
            self._log_timeout_error(timeout_err)
        return rates
    
    def get_rates_for_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> List[DatedRates]:
        rates: Optional[DatedRates] = None
        try:
            rates = self._client.get_rates_for_date_range(
                start_date = start_date, end_date = end_date
            )
        except requests.ConnectionError as conn_err:
            self._log_connection_error(conn_err)
        except requests.Timeout as timeout_err:
            self._log_timeout_error(timeout_err)
        return rates