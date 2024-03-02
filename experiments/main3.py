"""main3.py - Watch a portfolio of stocks and options, update at a specified interval.

Usage:
    main3.py --filepath=<filepath>


?? - On disk representation, to be read in and updated by IRepository.
IRepository - In memory representation with reader/writer facilities.
ICatalog - Contains a key-value store of IRepository objects.

Pipeline - Contains a list of steps to be executed in order. This includes predefined steps and user-defined steps.
"""
from abc import ABC

import click
import pandas as pd

from pipeline2.market_data.yahoo import Yahoo


class IRepository(ABC):
    def load(self):
        pass

    def save(self):
        pass

    def update(self):
        pass


class ICatalog(ABC):
    def get(self, key):
        pass

    def set(self, key, value):
        pass


class PortfolioCatalog(ICatalog):
    """Holds in-memory representations of portfolios and related data."""

    def __init__(self):
        self.items = {}

    def get(self, key):
        if key not in self.items:
            return None
        return self.items[key]

    def set(self, key, value):
        self.items[key] = value


class Transaction:
    """A single transaction for a given security."""

    def __init__(self, ticker, quantity, price, date, action):
        self.ticker = ticker
        self.quantity = quantity
        self.price = price
        self.date = date
        self.action = action

    def __repr__(self):
        return f"Transaction({self.ticker}, {self.quantity}, {self.price}, {self.date}, {self.action})"


class Position:
    """A set of transactions for a given security."""

    def __init__(self, ticker, transactions):
        self.ticker = ticker
        self.transactions = transactions

    def __repr__(self):
        return f"Position({self.ticker}, {self.transactions})"

    @property
    def time_held(self):
        return (self.transactions[-1].date - self.transactions[0].date).days

    @property
    def cost_basis(self):
        return sum([transaction.quantity * transaction.price for transaction in self.transactions])


class Portfolio(IRepository):
    def __init__(self, filepath):
        self.filepath = filepath
        self.df = None
        self.load()

    def load_csv(self) -> pd.DataFrame:
        df = pd.read_csv(self.filepath, parse_dates=["date".title()], converters={"price".title(): lambda x: float(x.replace('$', '').replace(',', ''))})
        df.columns = df.columns.str.lower()
        return df

    def load(self):
        self.df = self.load_csv()

    def save(self):
        self.df.to_csv(self.filepath)

    def update(self):
        # Check if the file has changed
        df = self.load_csv()

        if not self.df.equals(df):
            self.save()

    @property
    def tickers(self) -> list[str]:
        return self.df["ticker"].tolist()

    @property
    def cost_basis(self):
        return (self.df["quantity"] * self.df["price"]).sum()

    @property
    def positions(self):
        """Return a list of positions."""
        return [self.get_position(ticker) for ticker in self.tickers]

    def get_position(self, ticker):
        return Position(ticker, [Transaction(row["ticker"], row["quantity"], row["price"], row["date"], row["action"]) for _, row in self.df.iterrows() if row["ticker"] == ticker])


class Step(ABC):
    def execute(self, catalog):
        pass


class UpdatePortfolio(Step):
    def __init__(self, filepath):
        self.filepath = filepath

    def execute(self, catalog):
        portfolio = catalog.get("portfolio")
        if portfolio is None:
            catalog.set("portfolio", Portfolio(self.filepath))
        else:
            portfolio.update()


class Pipeline:
    def __init__(self, catalog):
        self.execution_points = {}
        self.catalog = catalog

    def add_step(self, step, execution_point="default"):
        if execution_point not in self.execution_points:
            self.execution_points[execution_point] = []
        self.execution_points[execution_point].append(step)

    def run(self):
        for execution_point, steps in self.execution_points.items():
            for step in steps:
                print(f"Executing {step.__class__.__name__} - {execution_point}")
                step.execute(self.catalog)

    def show_execution_order(self):
        i = 0
        for key, steps in self.execution_points.items():
            for step in steps:
                print(f"{i}. {step.__class__.__name__} - {key}")
                i += 1


class GetMarketData:
    """Get market data for all positions in the portfolio."""

    def execute(self, catalog):
        portfolio = catalog.get("portfolio")
        data = Yahoo().download_historical_data(list(set(portfolio.tickers)))

        # TODO inspect output of download_historical_data and massage into a DataFrame

        catalog.set("market_data", data)

class CalculatePortfolioStats:
    """Calculate portfolio statistics; total cost basis, total market value, total gain/loss ($/%), etc."""

    def execute(self, catalog):
        market_data = catalog.get("market_data")
        print(market_data)
        portfolio = catalog.get("portfolio")
        portfolio_stats = {
            "total_cost_basis": sum([position.cost_basis for position in portfolio.positions]),
            "total_market_value": sum([market_data[ticker]["Close"].iloc[-1] for ticker in portfolio.tickers]),
            "total_gain_loss": 0,
            "total_gain_loss_pct": 0,
        }
        catalog.set("portfolio_stats", portfolio_stats)


class CalculateStatsByTicker:
    """Calculate statistics for each position in the portfolio; cost basis, average time held, etc."""

    def execute(self, catalog):
        portfolio = catalog.get("portfolio")
        market_data = catalog.get("market_data")
        stats_by_ticker = {}
        for ticker in portfolio.tickers:
            position = portfolio.get_position(ticker)
            stats = {}
            if position.status == "closed":
                # TODO complete this
                # A position's stats are different when it is closed.
                # Specifically, the cost_basis is the total buying cost of the position, and the gain_loss is the
                # difference between the cost_basis and the selling price.
                pass
            else:
                stats_by_ticker[ticker] = {
                    "cost_basis": portfolio.get_position(ticker).cost_basis,
                    "average_time_held": portfolio.get_position(ticker).time_held,
                    "gain_loss": portfolio.get_position(ticker).cost_basis - market_data[ticker]["Close"].iloc[-1],
                    "gain_loss_pct": portfolio.get_position(ticker).cost_basis / market_data[ticker]["Close"].iloc[-1],
                }

        catalog.set("stats_by_ticker", stats_by_ticker)


class ReportPortfolioStats:
    """Report portfolio statistics; total cost basis, total market value, total gain/loss ($/%), etc."""

    def execute(self, catalog):
        portfolio_stats = catalog.get("portfolio_stats")
        print(portfolio_stats)


class ReportStatsByTicker:
    """Report statistics for each position in the portfolio; cost basis, average time held, etc."""

    def execute(self, catalog):
        stats_by_ticker = catalog.get("stats_by_ticker")
        print(stats_by_ticker)


@click.command()
@click.argument('filepath', type=click.Path(exists=True))
def main(filepath):
    # This will be shared between the various steps in the pipeline2.
    catalog = PortfolioCatalog()

    # init pipeline2
    pipeline = Pipeline(catalog)

    # add steps to pipeline2
    # Add predefined steps
    # 1. update portfolio if the file has changed
    pipeline.add_step(UpdatePortfolio(filepath), "default")
    # 2. get market data
    pipeline.add_step(GetMarketData(), "default")
    # 4. calculate stats for each position: cost basis, average time held, etc.
    pipeline.add_step(CalculatePortfolioStats(), "default")
    # 5. calculate stats for each position: cost basis, average time held, etc.
    pipeline.add_step(CalculateStatsByTicker(), "default")
    # 5. report portfolio stats
    pipeline.add_step(ReportPortfolioStats(), "default")
    # 6. report stats by ticker
    pipeline.add_step(ReportStatsByTicker(), "default")

    pipeline.show_execution_order()

    # run pipeline2
    pipeline.run()


if __name__ == '__main__':
    main()
