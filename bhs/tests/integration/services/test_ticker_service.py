import pandas as pd

from bhs.config import constants
from bhs.services import ticker_service


def test_compose_ticker_list():
    # Arrange
    # Act
    df = ticker_service.compose_ticker_list()

    df.to_parquet(constants.FAT_TICKERS_PATH)

    # Assert
    assert (df.shape[0] > 0)


def test_get_tickers():
    df = ticker_service.get_fat_tickers()

    print(df.shape[0])
