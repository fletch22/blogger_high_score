from datetime import datetime, timedelta

import pandas as pd
import pytz
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from bhs.config import constants

TZ_AMERICA_NEW_YORK = 'America/New_York'

STANDARD_DAY_FORMAT = '%Y-%m-%d'
US_DATE_FORMAT = '%m/%d/%Y'
STANDARD_DATE_WITH_SECONDS_FORMAT = '%Y-%m-%d_%H-%M-%S'
WTD_DATE_WITH_SECONDS_FORMAT = '%Y-%m-%d %H:%M:%S'
TWITTER_FORMAT = '%Y%m%D%H%M'
TIPRANKS_FORMAT = "%a %b %d %Y"
TIPRANKS_FORMAT_V2 = "%Y-%m-%dT%H:%M:%S.%fZ" # 2021-02-02T06:00:00.000Z

TWITTER_LONG_FORMAT = "%a %b %d %H:%M:%S %z %Y"

stock_market_holidays = ["2020-01-01", "2020-01-20", "2020-01-17", "2020-04-10", "2020-05-25",
                         "2020-07-03", "2020-09-07", "2020-11-26", "2020-12-25"]

NASDAQ_CLOSE_AT_ONE_PM_DATES = ["2020-11-27", "2020-12-24"]


def parse_twitter_date_string(date_string: str):
    return datetime.strptime(date_string, TWITTER_LONG_FORMAT).timestamp()


def parse_std_datestring(datestring):
    return datetime.strptime(datestring, STANDARD_DAY_FORMAT)


def get_us_mdy_format(date: datetime):
    return date.strftime(US_DATE_FORMAT)


def get_standard_ymd_format(date: datetime):
    return date.strftime(STANDARD_DAY_FORMAT)


def format_file_system_friendly_date(date: datetime):
    millis = round(date.microsecond / 1000, 2)
    return f"{date.strftime(STANDARD_DATE_WITH_SECONDS_FORMAT)}-{millis}"


def convert_wtd_nyc_date_to_std(date_str: str):
    dt_nyc = datetime.strptime(date_str, WTD_DATE_WITH_SECONDS_FORMAT)
    nyc_t = pytz.timezone(TZ_AMERICA_NEW_YORK)
    return nyc_t.localize(dt_nyc, is_dst=None)


def convert_wtd_nyc_date_to_utc(date_str: str):
    dt_nyz_tz = convert_wtd_nyc_date_to_std(date_str)
    dt_utc = dt_nyz_tz.astimezone(pytz.utc)

    return dt_utc


def convert_utc_timestamp_to_nyc(utc_timestamp):
    dt_utc = datetime.fromtimestamp(utc_timestamp)
    return dt_utc.astimezone(pytz.timezone(TZ_AMERICA_NEW_YORK))


def get_nasdaq_close_on_date(dt_utc: datetime):
    dtc_nyc = dt_utc.astimezone(pytz.timezone(TZ_AMERICA_NEW_YORK))

    # NOTE: 2020-09-26: chris.flesche: String time off - make midnight
    date_str = get_standard_ymd_format(dtc_nyc)
    dt_closed = parse_std_datestring(date_str)

    # NOTE: 2020-09-26: chris.flesche: Add timezone because above did not.
    tz = pytz.timezone(str(dtc_nyc.tzinfo))
    dt_closed = tz.localize(dt_closed)

    # NOTE: 2020-09-26: chris.flesche: Adjust if today's date has a special close date.
    hours_after_midnight = 13 if date_str in NASDAQ_CLOSE_AT_ONE_PM_DATES else 16
    dt_closed = dt_closed + timedelta(hours=hours_after_midnight)

    # NOTE: 2020-09-26: chris.flesche: Always convert back to utc
    return dt_closed.astimezone(pytz.utc)


def is_after_nasdaq_closed(utc_timestamp: int):
    dt_utc_tz_unaware = datetime.fromtimestamp(utc_timestamp)
    dt_utc = dt_utc_tz_unaware.astimezone(pytz.utc)

    dt_close = get_nasdaq_close_on_date(dt_utc=dt_utc)
    return dt_utc > dt_close


@udf(returnType=StringType())
def convert_timestamp_to_nyc_date_str(utc_timestamp):
    dt_nyc = convert_utc_timestamp_to_nyc(utc_timestamp=utc_timestamp)
    return get_standard_ymd_format(dt_nyc)


def get_nasdaq_trading_days_from(dt: datetime, num_days: int):
    time_dir = -1
    if num_days > 0:
        time_dir = 1
    for n in range(abs(num_days)):
        while True:
            dt = dt + timedelta(days=time_dir)
            if not is_stock_market_closed(dt):
                break
    return dt


def parse_tipranks_dt(date_str: str):
    return datetime.strptime(date_str, TIPRANKS_FORMAT_V2)


def get_next_market_date(date_str: str, is_reverse: bool = False) -> str:
    dt = parse_std_datestring(date_str)
    return get_standard_ymd_format(find_next_market_open_day(dt, is_reverse=is_reverse))


def find_next_market_open_day(dt: datetime, is_reverse: bool = False):
    day = -1 if is_reverse else 1
    while True:
        dt = dt + timedelta(days=day)
        is_closed, reached_end_of_data = is_stock_market_closed(dt)
        if reached_end_of_data:
            raise Exception("While finding the next market open day, the system reached the end of the available data.")
        if not is_closed:
            break
    return dt


def is_stock_market_closed(dt: datetime):
    date_str = get_standard_ymd_format(dt)
    max_date = sorted(get_market_holidays())[-1]
    reached_end_of_data = False
    if date_str > max_date:
        reached_end_of_data = True
    is_closed = False
    if dt.weekday() > 4:
        is_closed = True
    else:
        if date_str in get_market_holidays():
            is_closed = True
    return is_closed, reached_end_of_data


def get_market_holidays() -> str:
    global stock_market_holidays
    if stock_market_holidays is None:
        stock_market_holidays = pd.read_csv(constants.US_MARKET_HOLIDAYS_PATH)["date"].to_list()

    return stock_market_holidays


def get_days_between(date_str_1: str, date_str_2: str):
    dt_1 = parse_std_datestring(date_str_1)
    dt_2 = parse_std_datestring(date_str_2)
    return abs((dt_2 - dt_1).days)