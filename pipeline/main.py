"""main.py - simple data processing pipeline2 using nodes.

Examples:

--- User CL input string concat.

# Create Nodes
intput_node = InputNode(None, str)
str_concat = StrConcat(str, str)
output_node = OutputNode(str, None)

# Register Nodes
app.register(intput_node, as_input=True)
app.register(str_concat)
app.register(output_node, as_output=True)

# Connect Nodes
app.connect(intput_node, str_concat)
app.connect(str_concat, output_node)

# Print Graph
app.graph.print_graph()

# Run App
app.run()

---

"""
from typing import Type, List

import pandas as pd

from node import Node
from pipeline2.base_processing.get_tickers import GetTickers
from portfolio.csv import CsvPortfolio
from pipelinerunner import PipelineRunner


class OutputNode(Node):
    def __init__(self, is_output: bool = True):
        super().__init__(List, None, is_output=is_output)

    def process(self, _input: str) -> None:
        print(_input)


def main():
    """
    portfolio (csv)
        |
        v
    portfolio_process ----------------------------,
        |                                         |
        |                                         |
        v                                         |
    split_tickers                                 |
        |                                         |
        v                                         |
    (List[Ticker])                                |
        |                                         |
        v                                         |
    yahoo (web)                                   |
        |                                 (whole dataframe)
        v                                         |
    yahoo_process                                 |
        |                                         |
    (List[Dict[Ticker,Market Value]])             |
        |                                         |
        v                                         |
    get_cost_basis <-----------------------------`
        |
        v
    create_report
        |
        v
    format_report
    """
    app = PipelineRunner()

    # Create Nodes
    portfolio = CsvPortfolio(file_path="/home/albert/Finances/example_portfolio.csv",
                             col_check=["Ticker", "Date", "Action", "Quantity", "Price"])
    get_tickers = GetTickers()
    # split_ohlcv = GetOhlcv(list, list, is_output=True)
    output = OutputNode(is_output=True)

    # Register Nodes
    app.register_nodes([portfolio,
                        get_tickers,
                        # get_ohlcv,
                        output])

    # Connect Nodes
    app.connect_source(portfolio, get_tickers)
    app.connect_source(get_tickers, output)

    # Run App
    app.run()


if __name__ == '__main__':
    main()
