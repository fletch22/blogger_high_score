from datetime import timedelta
from types import SimpleNamespace
from typing import List, Dict

import pandas as pd
from pandas import DataFrame

from bhs.config import constants, logger_factory
from bhs.services import file_service
from bhs.utils import date_utils

logger = logger_factory.create_instance(__name__)


def get_all_tickers():
    da_paths = file_service.walk(constants.SHAR_SPLIT_EQUITY_EOD_DIR)
    return [d.stem for d in da_paths]


def get_ticker_eod_data(ticker: str) -> DataFrame:
    ticker_path = file_service.get_eod_ticker_file_path(ticker)
    df = None
    if ticker_path.exists():
        df = pd.read_csv(str(ticker_path))

    return df


def get_ticker_info():
    return pd.read_csv(constants.SHAR_TICKER_DETAIL_INFO_PATH)


def get_big_exchange_equities():
    df = get_ticker_info()
    return df[df["exchange"].isin(["NASDAQ", "NYSE"])]


def compose_ticker_list():
    tickers = get_tickers_w_filters()

    tq_list = [{"ticker": t.ticker, "volume": t.volume} for t in tickers]
    df_tickers = pd.DataFrame(tq_list)

    df_big_eq = get_big_exchange_equities()
    return df_tickers.merge(df_big_eq, on="ticker", how="inner") \
        .sort_values(by=["volume"], ascending=False) \
        [["ticker"]].drop_duplicates("ticker")


def get_tickers_w_filters(min_price: float = 5.0, min_volume: int = 50000) -> List[SimpleNamespace]:
    logger.info("About to walk file.")
    da_paths = file_service.walk(constants.SHAR_SPLIT_EQUITY_EOD_DIR)

    tickers = []
    for d in da_paths:
        ticker = d.stem
        df = get_ticker_eod_data(ticker)
        row = df.iloc[-1]
        price = row["close"]
        volume = row["volume"]
        if price > min_price and volume > min_volume:
            ns = SimpleNamespace(ticker=ticker, volume=volume)
            tickers.append(ns)

    return tickers


def get_fat_tickers():
    return pd.read_parquet(constants.FAT_TICKERS_PATH)


def get_equities_in_dates(tickers: List[str], start_dt_str: str, end_dt_str):
    all_tickers = []
    for t in tickers:
        df_t = get_equity_on_start_end(ticker=t, start_dt_str=start_dt_str, end_dt_str=end_dt_str)
        all_tickers.append(df_t)

    return pd.concat(objs=all_tickers, axis=0)


def get_equity_on_start_end(ticker: str, start_dt_str: str, end_dt_str: str, num_days_in_future: int = 1) -> pd.DataFrame:
    df = get_ticker_eod_data(ticker)
    df_in_range = None
    if df is not None:
        df_in_range = df[(df["date"] >= start_dt_str) & (df["date"] <= end_dt_str)].sort_values(by="date")
        df_in_range["future_open"] = df_in_range["open"]
        df_in_range["future_low"] = df_in_range["low"]
        df_in_range["future_high"] = df_in_range["high"]
        df_in_range["future_close"] = df_in_range["close"]
        df_in_range["future_date"] = df_in_range["date"]
        cols = ["future_open", "future_low", "future_high", "future_close", "future_date"]
        df_in_range[cols] = df_in_range[cols].shift(-num_days_in_future)

    return df_in_range


def get_equity_on_dates(ticker: str, date_strs: List[str],
                        num_days_in_future: int = 1) -> pd.DataFrame:
    df = get_ticker_eod_data(ticker)
    df_in_dates = None
    if df is not None:
        start, end = get_start_end_dates(date_strs)
        df_in_range = df[(df["date"] >= start) & (df["date"] <= end)].sort_values(by="date")
        df_in_range["future_open"] = df_in_range["open"]
        df_in_range["future_low"] = df_in_range["low"]
        df_in_range["future_high"] = df_in_range["high"]
        df_in_range["future_close"] = df_in_range["close"]
        df_in_range["future_date"] = df_in_range["date"]
        cols = ["future_open", "future_low", "future_high", "future_close", "future_date"]
        df_in_range[cols] = df_in_range[cols].shift(-num_days_in_future)

        df_in_dates = df_in_range[df_in_range["date"].isin(date_strs)]
    return df_in_dates


def get_start_end_dates(date_strs: List[str]):
    date_strs = sorted(date_strs)
    start_date = date_strs[0]
    end_date = date_strs[-1]

    dt = date_utils.parse_std_datestring(end_date)

    # NOTE: 2020-12-12: chris.flesche: Pad with 4 days in case of long weekends
    dt_end_adjust = dt + timedelta(days=4)
    end_date_adj = date_utils.get_standard_ymd_format(dt_end_adjust)

    return start_date, end_date_adj


def get_ticker_on_dates(tick_dates: Dict[str, List[str]], num_days_in_future: int = 1) -> pd.DataFrame:
    df_list = []
    for ticker, date_strs in tick_dates.items():
        df_equity = get_equity_on_dates(ticker=ticker, date_strs=date_strs, num_days_in_future=num_days_in_future)
        if df_equity is not None:
            df_list.append(df_equity)
    df_ticker = pd.concat(df_list).dropna(subset=["future_open", "future_low", "future_high", "future_close", "future_date"])

    return df_ticker