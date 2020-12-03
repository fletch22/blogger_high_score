from bhs.config import constants
from bhs.services import tip_ranks_service
from bhs.services.tip_ranks_service import AnalystTip


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
    tickers = ["fis", "aapl"]
    file_path = constants.ANALYST_STOCK_PICKS_FROM_TICKER_PATH

    # Act
    stock_tips = tip_ranks_service.scrap_tipranks(tickers=tickers, output_path=file_path)

    for ticker, tips in stock_tips.items():
        print(f"\nTicker: {ticker}\n")

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
    at = get_sample_analyst_tip()

    # Act
    print(at.__dict__)
    # Assert
