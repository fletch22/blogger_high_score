import pandas as pd

from bhs.config import logger_factory, constants
from bhs.pipeline.tipranks import pipe_agg

logger = logger_factory.create_instance(__name__)

pd.set_option('display.max_rows', 50000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def test_agg_tipranks():
    pipe_agg.agg_tipranks()


# def test_add_rolling_perf():
#     df = pd.read_parquet(constants.TIP_RANKS_SCORED_FULL)
#     df = df.sample(frac=.2)
#
#     df = pipe_agg.add_rolling_perf(df=df)
#
#     df.sort_values(by=["rolling_analyst_score"], ascending=False, inplace=True)
#     logger.info(df[["analyst_name", "above_prev_target_price", "rolling_analyst_score"]].head(100))