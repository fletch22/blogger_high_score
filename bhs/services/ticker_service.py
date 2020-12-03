from bhs.config import constants
from bhs.services import file_service


def get_all_tickers():
    da_paths = file_service.walk(constants.SHAR_SPLIT_EQUITY_EOD_DIR)
    return [d.stem for d in da_paths]
