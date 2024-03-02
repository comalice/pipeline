"""Source financial data from Yahoo Finance."""
import time
from random import random
from typing import List

import pandas as pd
import yfinance as yf
from abc import ABC, abstractmethod
from .IDataSource import IDataSource, exponential_backoff
from ..node import Node


class Yahoo(IDataSource):
    """A source for financial data from Yahoo Finance."""

    def __init__(self):
        pass

    def get_securities(self):
        """Get a list of all securities available from the source."""
        # TODO https://quant.stackexchange.com/q/1640
        raise NotImplementedError

    def download_historical_data(self, tickers, period="1d", max_retries=5, base_delay=1, jitter=0.1):
        """Downloads historical data for a list of tickers with exponential backoff."""
        data = {}
        for ticker in set(tickers):
            data[ticker] = self.download_ticker_data(ticker, period, max_retries, base_delay, jitter)
        return data

    def download_ticker_data(self, ticker, period="1d", max_retries=5, base_delay=1, jitter=0.1):
        """Downloads historical data for a single ticker with exponential backoff."""
        return exponential_backoff(max_retries, base_delay, jitter, yf.download, ticker, period=period, auto_adjust=True)

    def get_latest_price(self, ticker):
        """Gets the latest price for a single ticker."""
        # return yf.Ticker(ticker).history(period="1d")["Close"].iloc[-1]
        return exponential_backoff(5, 1, 0.1, self._get_latest_price, ticker)

    def _get_latest_price(self, ticker):
        return yf.Ticker(ticker).history(period="1d")["Close"].iloc[-1]


class YahooNode(Node):
    def __init__(self, period: str = "1d"):
        super().__init__(None, pd.DataFrame)
        self.period = period

    def process(self, _input: List[str]) -> dict[any, any]:
        """Download historical data for a list of tickers."""
        return Yahoo().download_historical_data(_input, self.period)


class YahooProcessor(Node):
    def __init__(self, period: str = "1d"):
        super().__init__(dict[any, any], pd.DataFrame)
        self.period = period

    def process(self, _input: dict[any, any]) -> pd.DataFrame:
        """Download historical data for a list of tickers."""
        return pd.DataFrame(_input)
