import math
import statistics
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bhs.config import constants, logger_factory
from bhs.utils import date_utils

COL_MONTH_STD = "month_std_dt"

logger = logger_factory.create_instance(__name__)


def calc_roi(inv, close, fut_close):
    roi = (fut_close - close) / close
    return roi, inv + (inv * roi)


def get_ranges_months(start_std_dt: str, end_std_dt: str):
    start_date = date_utils.parse_std_datestring(start_std_dt)
    end_date = date_utils.parse_std_datestring(end_std_dt)
    num_months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)

    collection = []
    for month in range(num_months):
        month += 1
        year = start_date.year
        end_dt_year = year
        if month > 12:
            month -= 12
        end_month = month + 1
        if month == 12:
            end_dt_year = year + 1
            end_month = 1
        collection.append((f"{start_date.year}-{month:02}-01", f"{end_dt_year}-{end_month:02}-01"))

    return collection


def backtest():
    # NOTE: 2021-07-24: chris.flesche: Better performance: TIP_RANKS_SCORED_FULL; Also, TIP_RANKS_STOCK_DATA_PATH
    source_path = constants.TIP_RANKS_SCORED_FULL
    purch_price_col = "close_1"  # close_1 # close
    purch_dt_col = "close_dt_1"
    future_close_col = "close_3"  # close_day_2 # future_close
    future_close_dt = "close_dt_3"  # close_day_2 # future_close

    df = pd.read_parquet(source_path)

    logger.info(f"Size of df: {df.shape[0]}")
    logger.info(f"Size of df price col isnan: {df[np.isnan(df[purch_price_col])].shape[0]}")

    df.loc[:, :] = df[~np.isnan(df[future_close_col])]
    df.loc[:, :] = df[~np.isnan(df[purch_price_col])]

    df[purch_dt_col] = df[purch_dt_col].replace(np.nan, '', regex=True)
    df[purch_dt_col] = df[purch_dt_col].fillna('')
    df[purch_dt_col] = df[purch_dt_col].astype(str)
    df[purch_dt_col] = df[purch_dt_col].replace('nan', '', regex=True)
    df = df[df[purch_dt_col] != ""].copy()

    frac = 1.
    df = df.sample(frac=frac)

    # logger.info(f"Cols: {df.columns}")

    min_vol_metric = 5 * 250000
    init_inv = 10000
    tar_price_rat_threshold = .3

    # start_std_dt = "2016-07-01"
    # end_std_dt = "2021-08-01"
    # dt_ranges = [(start_std_dt, end_std_dt)]

    min_std_dt = df[purch_dt_col].min()
    max_std_dt = df[purch_dt_col].max()
    logger.info(f"{min_std_dt}:{max_std_dt}")
    dt_ranges = get_ranges_months(start_std_dt=min_std_dt, end_std_dt=max_std_dt)

    timespan_roi = []
    for dt in dt_ranges:
        inv, roi_all, tickers_hit = roll_the_die(df=df,
                                                 fut_close_col=future_close_col,
                                                 fut_close_dt_col=future_close_dt,
                                                 init_inv=init_inv,
                                                 min_vol_metric=min_vol_metric,
                                                 purch_dt_col=purch_dt_col,
                                                 purch_price_col=purch_price_col,
                                                 tar_price_rat_threshold=tar_price_rat_threshold,
                                                 start_std_dt=dt[0],
                                                 end_std_dt=dt[1])

        if len(roi_all) > 0:
            mean_roi = statistics.mean([roi for dt, roi in roi_all])
            timespan_roi.append(mean_roi)
            logger.info(f"Mean roi: {mean_roi:.5}; num datapoints: {len(roi_all)}")
            # logger.info(f"Investment: {inv}")
            # logger.info(f"Tickers: {tickers_hit}")
            # chart_month_counts([dt for dt, roi in roi_all])
        else:
            timespan_roi.append(0)
            logger.info("No data with the parameters specified.")

        # break

    x_axis_labels = list(range(len(timespan_roi)))
    chart_timespan_rois(timespan_rois=timespan_roi,
                        x_axis_labels=x_axis_labels)


def roll_the_die(df: pd.DataFrame,
                 fut_close_col: str,
                 fut_close_dt_col: str,
                 init_inv: float,
                 min_vol_metric: int,
                 purch_dt_col: str,
                 purch_price_col: str,
                 tar_price_rat_threshold: float,
                 start_std_dt: str,
                 end_std_dt: str):

    logger.info(f"Purchase date range: {start_std_dt}:{end_std_dt}")
    df = df[(df[purch_dt_col] > start_std_dt) & (df[purch_dt_col] < end_std_dt)].copy()
    df_grouped_by = df.groupby("ticker")

    inv = init_inv
    roi_all = []
    tickers_hit = set()
    for ticker, df_group in df_grouped_by:
        for ndx, row in df_group.iterrows():
            tar_price = row["target_price"]
            price = row[purch_price_col]
            fut_close = row[fut_close_col]
            fut_close_dt = row[fut_close_dt_col]
            vol = row["volume"]
            rat_dt = row["date"]
            purchase_dt = row[purch_dt_col]
            vol_metric = price * vol

            tar_price_rat = (tar_price - price) / price

            if tar_price_rat > tar_price_rat_threshold and vol_metric > min_vol_metric:
                roi, inv = calc_roi(inv, price, fut_close)
                # logger.info(f"{ticker}; {inv:.2f}; {price} ({rat_dt}); {fut_close} ({fut_close_dt})")
                roi_all.append((purchase_dt, roi))
                tickers_hit.add(ticker)
            if inv <= 0:
                break

        if inv <= 0:
            break
    return inv, roi_all, tickers_hit


def convert_to_months_from_start(start_std_dt, std_end_dt):
    start_dt = date_utils.parse_std_datestring(start_std_dt)
    end_date = date_utils.parse_std_datestring(std_end_dt)

    return math.floor(((end_date - start_dt).days / 365.25) * 12 + (end_date.month - start_dt.month)) + 1


def convert_to_months_std_dt(std_dt):
    return std_dt[:-3]


def add_std_month_and_count(df, std_dt_col):
    return df


def chart_month_counts(raw_dates: List[str]):
    dict_list = []
    for r in raw_dates:
        dict_list.append(dict(col_1=r))

    df = pd.DataFrame(dict_list)

    col_2_count = "col_1"

    df[COL_MONTH_STD] = df[col_2_count].apply(lambda std_dt: convert_to_months_std_dt(std_dt))
    df = df.groupby(by=COL_MONTH_STD)[COL_MONTH_STD].count().reset_index(name="count")

    month_std_list = df[COL_MONTH_STD]
    count_list = df["count"]

    # logger.info(df.head())
    fig = plt.figure()
    x = month_std_list
    height = count_list
    width = 1.0
    plt.bar(x, height, width, color='b')
    plt.xticks(rotation=45)
    fig.subplots_adjust(bottom=0.15)
    plt.show()


def chart_timespan_rois(timespan_rois: List[str], x_axis_labels: List[str]):
    fig = plt.figure()
    y_heights = timespan_rois
    width = 1.0
    plt.bar(x_axis_labels, y_heights, width, color='b')
    plt.xticks(rotation=45)
    fig.subplots_adjust(bottom=0.15)
    plt.show()


if __name__ == '__main__':
    backtest()