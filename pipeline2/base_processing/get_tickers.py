"""get_tickers.py - given a backing DataFrame, return a list of tickers."""
from typing import List

import pandas as pd

from pipeline2.node import Node


class GetTickers(Node):
    def __init__(self):
        super().__init__(pd.DataFrame, List)

    def process(self, _input: pd.DataFrame) -> List:
        return _input['Ticker'].tolist()
