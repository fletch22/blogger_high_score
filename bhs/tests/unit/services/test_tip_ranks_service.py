import pandas as pd

from bhs.config import constants, logger_factory
from bhs.services import tip_ranks_service, ticker_service
from bhs.services.tip_ranks_service import AnalystTip
from bhs.utils import date_utils

pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

logger = logger_factory.create_instance(__name__)


def test_login():
    # Arrange
    driver = tip_ranks_service.get_driver()

    # Act
    driver = tip_ranks_service.login(driver)

    # Assert
    # print(str(elem))


def test_login_and_get_stocks():
    # Arrange
    # tickers = ["fis", "doesnotexist", "aapl"]
    # tickers = ["fis", "aapl"]
    df = ticker_service.get_fat_tickers()
    tickers = df["ticker"].to_list()
    file_path = constants.ANALYST_STOCK_PICKS_FROM_TICKER_PATH

    # Act
    stock_tips = tip_ranks_service.scrape_tipranks(tickers=tickers, output_path=file_path)

    for ticker, tips in stock_tips.items():
        print(f"Ticker: {ticker}")

    # Assert
    assert (len(stock_tips.keys()) > 0)


def get_sample_analyst_tip():
    at = AnalystTip()
    at.tr_rating_roi = "15%"
    at.target_price = "123.45"
    at.rating_initiating = "Initiating"
    at.dt_rating = "20 March 30"
    at.rating = "foo"
    at.org_name = "IBM"
    at.rank_raw = "Buy"
    at.analyst_name = "Joe Blow"

    return at


def test_analyst_tip():
    # Arrange
    file_path = constants.ANALYST_STOCK_PICKS_FROM_TICKER_PATH

    import pandas as pd
    df = pd.read_parquet(file_path)

    # Act
    print(df.shape[0])
    # Assert


def test_foo():
    import pandas as pd

    pd.set_option('display.max_rows', 5000)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    file_path = constants.ANALYST_STOCK_PICKS_FROM_TICKER_PATH

    import pandas as pd
    df = pd.read_parquet(file_path)

    # Act
    print(df.shape[0])

    def convert_dt(date_str):
        dt = date_utils.parse_tipranks_dt(date_str=date_str)
        return date_utils.get_standard_ymd_format(date=dt)

    df["dt_rating"] = df.apply(lambda x: convert_dt(x["dt_rating"]), axis=1)

    def convert_rank(rank_raw):
        return float(rank_raw.split(" ")[0])

    df["rank_raw"] = df["rank_raw"].fillna("2.5 out of 5")
    df["rank"] = df.apply(lambda x: convert_rank(x["rank_raw"]), axis=1)
    df_ranked = df.drop(columns=["rank_raw"])

    rank_unique_cols = ["ticker", "analyst_name", "dt_rating"]
    df_ranked = df_ranked.sort_values(by=rank_unique_cols).drop(columns="analyst_rel_url")

    print(df_ranked.head(100))


def test_data_file():
    # Arrange
    df = pd.read_parquet(constants.ANALYST_STOCK_PICKS_FROM_TICKER_PATH)

    df.sort_values(by=["ticker"], ascending=False, inplace=True)

    # Act
    logger.info(df.head())

    # Assert