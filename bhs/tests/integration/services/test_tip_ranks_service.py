from pathlib import Path

from bhs.config import constants, logger_factory
from bhs.services import tip_ranks_service, ticker_service
from bhs.services.tip_ranks_service import scrape_tipranks, get_driver

logger = logger_factory.create_instance(__name__)


def test_foo():
    df = ticker_service.get_fat_tickers()
    tickers = df["ticker"].to_list()
    # file_path = constants.ANALYST_STOCK_PICKS_FROM_TICKER_PATH
    tickers = sorted(tickers)


    # tickers = [t for t in tickers if t.startswith("B")]
    # tickers = ["BA"]
    file_path = None

    # Act
    stock_tips = scrape_tipranks(tickers=tickers, output_path=file_path)

    for ticker, tips in stock_tips.items():
        print(tips)
        print(f"Ticker: {ticker}")


def test_find_elements():
    # Arrange
    driver = get_driver()
    ticker = "foo"

    res_path = Path(constants.PROJECT_ROOT, "bhs", "tests", "resources")
    file_path = Path(res_path, "tip_ranks_stock_samp_1.html")
    driver.get(str(file_path))

    at_list = tip_ranks_service.find_all_data_on_page(driver=driver, ticker=ticker)

    # Act

    # Assert