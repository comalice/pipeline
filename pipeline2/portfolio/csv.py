"""portfolio/csv.py - Ingest a portfolio from a CSV file.

The CSV file should have the following columns: Ticker,Date,Action,Quantity,Price

Any other columns are not necessary, but will be included in the output for user convenience. If you need to process
those other columns, you can do so in a separate downstream node.
"""
import pandas as pd
from typing import Type, List

from pipeline2.node import Node


class CsvPortfolio(Node):
    def __init__(self, file_path: str, col_check: List[str] = None):
        super().__init__(None, pd.DataFrame)
        self.file_path = file_path
        self.col_check = col_check

    def process(self, _input: None) -> pd.DataFrame:
        """Read the CSV file and return a DataFrame."""
        df = pd.read_csv(self.file_path)

        # check if columns are in the dataframe
        if self.col_check is not None:
            cols_not_found = [col for col in self.col_check if col not in df.columns]
            if len(cols_not_found) > 0:
                raise ValueError(f"Columns not found in portfolio {self.file_path}: {cols_not_found}")

        # set data as index in the dataframe
        # Do we actually need to do this??
        df.set_index('Date', inplace=True)

        return df
