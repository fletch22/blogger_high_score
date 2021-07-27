from enum import Enum

import pandas as pd

from bhs.config import constants
from bhs.config import logger_factory
from bhs.services import ticker_service
from bhs.utils import date_utils

logger = logger_factory.create_instance(__name__)


class RatingMomentum(Enum):
    Reiterated = "Reiterated"
    Upgraded = "Upgraded"
    Initiated = "Initiated"
    Downgraded = "Downgraded"
    Assigned = "Assigned"
    Unrated = "Unrated"


# NOTE: 2021-04-04: chris.flesche: Rating: 1 == buy, 0==hold, -1==sell; This attempts to combine
# a cat field with the numeric field; the intent is to show the direction of movement
def assign_rating_momentum(rating: str, rating_initiating):
    rm = rating
    if rating_initiating == RatingMomentum.Reiterated.name:
        rm = 0
    elif rating_initiating == RatingMomentum.Upgraded.name:
        rm = 1
    elif rating_initiating == RatingMomentum.Downgraded.name:
        if rating == 0:
            rm = -1
        elif rating == -1:
            rm = -1

    return rm


def add_rolling_values(df: pd.DataFrame):
    df.loc[:, "rating_momentum"] = df.apply(lambda row: assign_rating_momentum(row["rating"], row["rating_initiating"]), axis=1)

    all_dfs = []
    with pd.option_context('mode.chained_assignment', None):
        df_grouped = df.groupby(by=["ticker"])

        for _, df_g in df_grouped:
            df_g.sort_values(by=["rating_date"], inplace=True)
            earliest_date_str = df_g["rating_date"].min()
            oldest_date_str = df_g["rating_date"].max()
            num_days = date_utils.get_days_between(date_str_1=earliest_date_str, date_str_2=oldest_date_str)
            num_days = 2 if num_days < 2 else num_days

            df_g.loc[:, "rating_momentum"] = df_g["rating_momentum"].rolling(window=num_days, min_periods=1).mean().astype("float64")
            df_g.loc[:, "target_price"] = df_g["target_price"].rolling(window=num_days, min_periods=1).mean().astype("float64")
            df_g.loc[:, "tr_rating_roi"] = df_g["tr_rating_roi"].rolling(window=num_days, min_periods=1).mean().astype("float64")
            df_g.loc[:, "rank"] = df_g["rank"].rolling(window=num_days, min_periods=1).mean().astype("float64")
            df_g.loc[:, "rating"] = df_g["rating"].rolling(window=num_days, min_periods=1).mean().astype("float64")
            all_dfs.append(df_g)

    return pd.concat(all_dfs, axis=0)


def agg_tipranks():
    df = pd.read_parquet(constants.TIP_RANKS_STOCK_DATA_PATH)

    df = add_rolling_values(df=df)
    df.rename(columns={"rating_date": "date"}, inplace=True)
    df.sort_values(by=["ticker", "date"], inplace=True)

    tickers = df["ticker"].unique()
    df_stocks = ticker_service.get_equities_in_dates(tickers=tickers, start_dt_str="2016-08-10", end_dt_str="2022-08-01")

    del_suffix = "_right"
    df_merged = pd.merge(df_stocks, df, how="inner", on=["ticker", "date"], suffixes=["", del_suffix])
    df_merged.sort_values(by=["ticker", "date"], inplace=True)

    to_delete_cols = [c for c in df_merged.columns if c.endswith(del_suffix)]
    df_merged = df_merged.drop(columns=to_delete_cols)

    df = df_merged

    df = add_rolling_perf(df=df)

    df.to_parquet(constants.TIP_RANKS_SCORED_FULL)

    return df


def add_rolling_perf(df: pd.DataFrame):
    df_name_g = df.groupby(by=["analyst_name", "ticker"])

    df_all = []
    for (a_name, ticker), df_n_g in df_name_g:
        df_n_g.sort_values(by=["date"], inplace=True)
        df_n_g["prev_target_price"] = df_n_g["target_price"]
        df_n_g[["prev_target_price"]] = df_n_g[["prev_target_price"]].shift(1)
        # NOTE: 2021-07-24: chris.flesche: Sell ratings == -1; therefore multiplying by rating turns a predicted loss
        # (from a SELL rating) into a positive score. Yay! Buy ratings == 1, so no effect.
        df_n_g["rating_score"] = df_n_g["rating"] * ((df_n_g["rating_date_price"] - df_n_g["prev_target_price"]) / df_n_g["prev_target_price"])

        df_all.append(df_n_g)

    df_above = pd.concat(df_all, axis=0)

    num_days = 2 * 365
    df_all = []
    df_name_g = df_above.groupby(by=["analyst_name"])

    for a_name, df_n_g in df_name_g:
        df_n_g.sort_values(by=["date"], inplace=True)
        df_n_g.loc[:, "rolling_analyst_score"] = df_n_g["rating_score"].rolling(window=num_days, min_periods=1).mean().astype("float64")
        df_all.append(df_n_g)

    return pd.concat(df_all, axis=0)


if __name__ == '__main__':
    agg_tipranks()