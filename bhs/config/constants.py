import os
from pathlib import Path

import yaml

from bhs.config.TwitterCredentials import TwitterCredentials

PROJECT_ROOT = Path(__file__).parent.parent.parent

app_name = "blogger_high_score"

def ensure_dir(dir):
    os.makedirs(dir, exist_ok=True)


if os.name == 'nt':
    WORKSPACE_PATH = Path('C:\\Users\\Chris\\workspaces\\')
    OVERFLOW_WORKSPACE_PATH = Path('D:\\workspaces\\')
else:
    # /home/jupyter/alpha_media_signal/
    WORKSPACE_PATH = Path('/home', 'jupyter')
    OVERFLOW_WORKSPACE_PATH = Path('/home', 'jupyter', 'overflow_workspace')
ensure_dir(str(OVERFLOW_WORKSPACE_PATH))

DATA_PATH = Path(WORKSPACE_PATH, 'data')
LOGGING_PATH = Path(DATA_PATH, 'logs', app_name)
ensure_dir(LOGGING_PATH)

if os.name == 'nt':
    RESOURCES_PATH = Path(DATA_PATH, 'credentials')
    TWITTER_CREDS_PATH = Path(RESOURCES_PATH, "search_tweets_creds.yaml")
    TIPRANKS_CREDS_PATH = Path(RESOURCES_PATH, "tipranks_creds.yaml")

    with open(TWITTER_CREDS_PATH) as file:
        # The FullLoader parameter handles the conversion from YAML
        # scalar values to Python the dictionary format
        twitter_creds = yaml.load(file, Loader=yaml.FullLoader)

    fletch22_key = 'search_tweets_fullarchive_development'
    standard_search_key = 'standard_search_tweets'
    rogd_key = "standard_search_tweets_rogd"

    TWITTER_FLETCH22_CREDS = TwitterCredentials(twitter_creds, fletch22_key)
    TWITTER_STANDARD_CREDS = TwitterCredentials(twitter_creds, standard_search_key)
    TWITTER_ROGD_CREDS = TwitterCredentials(twitter_creds, standard_search_key)

    CURRENT_CREDS = TWITTER_ROGD_CREDS

    with open(TIPRANKS_CREDS_PATH) as file:
        # The FullLoader parameter handles the conversion from YAML
        # scalar values to Python the dictionary format
        tipranks_creds = yaml.load(file, Loader=yaml.FullLoader)

    tipranks_username = tipranks_creds["username"]
    tipranks_password = tipranks_creds["password"]

TWITTER_OUTPUT_RAW_PATH = Path(DATA_PATH, 'twitter')
TWITTER_RAW_TWEETS_PREFIX = 'tweets_raw'

DATA_PATH = Path(WORKSPACE_PATH, 'data')
OVERFLOW_DATA_PATH = Path(OVERFLOW_WORKSPACE_PATH, 'data')

WEATHER_DATA_DIR = Path(OVERFLOW_DATA_PATH, "weather")
WEATHER_DATA_PATH = Path(WEATHER_DATA_DIR, "station_1.txt")

FIN_DATA = Path(OVERFLOW_DATA_PATH, 'financial')

TWITTER_OUTPUT = Path(OVERFLOW_DATA_PATH, 'twitter')
TWITTER_OUTPUT.mkdir(exist_ok=True)
TICKER_NAME_SEARCHABLE_PATH = Path(TWITTER_OUTPUT, 'ticker_names_searchable.csv')

TWITTER_TRASH_OUTPUT = Path(TWITTER_OUTPUT, "trash")
ensure_dir(TWITTER_TRASH_OUTPUT)

YAHOO_OUTPUT_PATH = Path(FIN_DATA, 'yahoo')
YAHOO_COMPANY_INFO = Path(YAHOO_OUTPUT_PATH, 'company_info')
ensure_dir(str(YAHOO_COMPANY_INFO))

QUANDL_TBLS_DIR = Path(FIN_DATA, 'quandl', 'tables')
SHAR_TICKERS = os.path.join(QUANDL_TBLS_DIR, "shar_tickers.csv")

TOP_100K_WORDS_PATH = Path(DATA_PATH, 'english', 'top100KWords.txt')

QUANDL_DIR = os.path.join(FIN_DATA, "quandl")
QUANDL_TBLS_DIR = os.path.join(QUANDL_DIR, "tables")
SHAR_CORE_FUNDY_FILE_PATH = Path(QUANDL_TBLS_DIR, "shar_core_fundamentals.csv")

SHARADAR_ACTIONS_DIR = os.path.join(QUANDL_TBLS_DIR, "shar_actions")
ensure_dir(SHARADAR_ACTIONS_DIR)
SHARADAR_ACTIONS_FILEPATH = Path(SHARADAR_ACTIONS_DIR, "actions.csv")

SHAR_SPLIT_EQUITY_EOD_DIR = Path(QUANDL_TBLS_DIR, "splits_eod")
SHAR_TICKER_DETAIL_INFO_PATH = Path(QUANDL_TBLS_DIR, "shar_tickers.csv")

KAFKA_URL = "localhost:9092"

TWITTER_INFERENCE_MODEL_PATH = Path(TWITTER_OUTPUT, "inference_model_drop")
TRAIN_READY_TWEETS = Path(TWITTER_INFERENCE_MODEL_PATH, "train_ready_tweets.mod")

BLOGGER_HIGH_SCORE__PROJECT_ROOT = Path(WORKSPACE_PATH, app_name)

BERT_PATH = Path(TWITTER_OUTPUT, "bert")
BERT_APPS_DATA_PATH = Path(BERT_PATH, "apps.csv")
BERT_REVIEWS_DATA_PATH = Path(BERT_PATH, "reviews.csv")

TWITTER_TEXT_LABEL_TRAIN_PATH = Path(TWITTER_INFERENCE_MODEL_PATH, "twitter_text_with_proper_labels.parquet")

TWITTER_MODEL_PATH = Path(TWITTER_INFERENCE_MODEL_PATH, "models")
ensure_dir(TWITTER_MODEL_PATH)

BLOG_HS_OUTPUT = Path(OVERFLOW_DATA_PATH, "blogger_high_score")
ensure_dir(BLOG_HS_OUTPUT)

PICKS_FROM_TICKER_PATH = Path(BLOG_HS_OUTPUT, "ticker")
ensure_dir(PICKS_FROM_TICKER_PATH)

ANALYST_STOCK_PICKS_FROM_TICKER_PATH = Path(PICKS_FROM_TICKER_PATH, "ticker_analyst_picks.parquet")

FAT_TICKERS_PATH = Path(BLOG_HS_OUTPUT, "fat_tickers.parquet")

TIP_RANKS_DATA_DIR = Path(FIN_DATA, 'tip_ranks')
ensure_dir(TIP_RANKS_DATA_DIR)
TIP_RANKS_STOCK_DATA_PATH = os.path.join(TIP_RANKS_DATA_DIR, "tip_ranks_stock.parquet")

US_MARKET_HOLIDAYS_PATH = Path(FIN_DATA, "us_market_holidays.csv")