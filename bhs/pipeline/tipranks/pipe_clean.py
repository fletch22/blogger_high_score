import gc
import sys

paths_to_add = ['/home/jovyan/work', '/home/jupyter/alpha_media_signal']

for p in paths_to_add:
    if p not in sys.path:
        sys.path.append(p)

import pandas as pd
from bhs.config import constants, logger_factory
from bhs.utils import date_utils
from bhs.services import ticker_service
from datetime import timedelta

pd.set_option('display.max_rows', 50000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

gc.collect()

logger = logger_factory.create_instance(__name__)


def convert_dt(date_str):
    dt = date_utils.parse_tipranks_dt(date_str=date_str)
    return date_utils.get_standard_ymd_format(date=dt)


def clean_rank(df):
    df_ranked = df.copy()
    df_ranked["rank_raw"] = df_ranked["rank_raw"].apply(lambda r: r if r != "unknown" else None)
    df_ranked["rank_raw"] = df_ranked["rank_raw"].fillna("2.5")
    df_ranked["rank"] = df_ranked["rank_raw"].astype(float)
    return df_ranked.drop(columns=["rank_raw"])


def convert_rating(rating_str):
    result = 0
    rating_str = rating_str.lower()
    if rating_str == "buy":
        result = 1
    elif rating_str == "sell":
        result = -1
    return result


def clean_rating(df):
    df["rating_raw"] = df["rating"]
    df["rating"] = df["rating_raw"].apply(convert_rating)
    return df.drop("rating_raw", axis=1)


def add_dates(df):
    df_fut_prep = df.copy()
    ranked_tickers = df_fut_prep["ticker"].unique()

    all_tick = []
    for rt in ranked_tickers:
        df = ticker_service.get_ticker_eod_data(rt)
        df_rt = df_fut_prep[df_fut_prep["ticker"] == rt]

        min_date = df_rt["dt_rating"].min()
        max_date = df_rt["dt_rating"].max()

        dt_max = date_utils.parse_std_datestring(max_date)
        dt_max = dt_max + timedelta(days=5)
        max_date = date_utils.get_standard_ymd_format(dt_max)

        df = df[(df["date"] > min_date) & (df["date"] < max_date)]
        future_dts = [1, 2, 3, 4, 5, 6, 7, 14, 28, 150]

        for fd in future_dts:
            close_col = f"close_{fd}"
            close_dt_col = f"close_dt_{fd}"
            df[close_dt_col] = df["date"]
            df[close_col] = df["close"]
            df[[close_col, close_dt_col]] = df[[close_col, close_dt_col]].shift(-fd)

        df_merged = pd.merge(df, df_rt, on=["ticker"], how='inner', suffixes=[None, "_eq_fun"])

        all_tick.append(df_merged)

    return pd.concat(all_tick, axis=0)


def replace_bad_target_price(row):
    result = row["target_price"]
    if result is None:
        result = row["close"] + .01
    return result


def clean_target_price(df):
    df_good_tp = df.copy()
    df_good_tp["target_price"] = df_good_tp.apply(replace_bad_target_price, axis=1).astype(float).copy()
    return df_good_tp


def clean_tr_rating_roi(df):
    df_good_roi = df.copy()

    df_good_roi["tr_rating_roi"] = df_good_roi["tr_rating_roi"].fillna("unknown")
    df_good_roi["tr_rating_roi"] = df_good_roi["tr_rating_roi"].astype(str)

    df_good_roi["tr_rating_roi"] = df_good_roi["tr_rating_roi"].str.replace("―\nUpside", "")
    df_good_roi["tr_rating_roi"] = df_good_roi["tr_rating_roi"].str.replace("<", "")
    df_good_roi["tr_rating_roi"] = df_good_roi["tr_rating_roi"].str.replace(">", "")
    df_good_roi["tr_rating_roi"] = df_good_roi["tr_rating_roi"].str.replace(" ", "0")
    df_good_roi["tr_rating_roi"] = df_good_roi["tr_rating_roi"].str.replace("None", "0")
    df_good_roi["tr_rating_roi"] = df_good_roi["tr_rating_roi"].str.replace("unknown", "0")
    df_good_roi["tr_rating_roi"] = df_good_roi["tr_rating_roi"].replace(r'^\s*$', "0", regex=True)
    df_good_roi["tr_rating_roi"] = df_good_roi["tr_rating_roi"].astype(float).copy()

    return df_good_roi


def clean_cols(df):
    df_ren = df.copy()
    df_ren.rename(columns={"date": "trade_date", "close": "rating_date_price", "dt_rating": "rating_date"}, inplace=True)
    df_ren.drop(columns=["closeunadj", "lastupdated"], inplace=True)
    df_ren.reset_index(drop=True, inplace=True)

    return df_ren


def clean_tip_ranks_raw():
    file_path = constants.ANALYST_STOCK_PICKS_FROM_TICKER_PATH
    df = pd.read_parquet(file_path)
    # df.head(100)

    df_rat = df.copy()
    df_rat["dt_rating"] = df_rat.apply(lambda x: convert_dt(x["dt_rating"]), axis=1)
    # df_rat.head(100)

    df_ranked = clean_rank(df=df_rat)
    rank_unique_cols = ["ticker", "analyst_name", "dt_rating"]
    df_ranked = df_ranked.sort_values(by=rank_unique_cols).drop(columns="analyst_rel_url")
    df_ranked = clean_rating(df=df_ranked)
    df_ranked["target_price"] = df_ranked["target_price"].astype(float)

    df_all = add_dates(df=df_ranked)
    stock_data_cols = ["ticker", "analyst_name", "dt_rating"]
    df_all = df_all[df_all["dt_rating"] >= df_all["date"]].copy()
    df_all.sort_values(by=stock_data_cols, inplace=True)
    df_dropped = df_all.drop_duplicates(subset=stock_data_cols, keep="last").copy()

    df_dropped[["ticker", "analyst_name", "dt_rating", "date", "close", "target_price"]].head()
    df_good_tp = clean_target_price(df=df_dropped)

    df_good_roi = clean_tr_rating_roi(df=df_good_tp)

    df_ren = df_good_roi.copy()
    df_ren = clean_cols(df=df_ren)

    df_ren.to_parquet(constants.TIP_RANKS_STOCK_DATA_PATH)


if __name__ == '__main__':
    clean_tip_ranks_raw()