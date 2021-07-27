# Blogger High Score

What are the best stock bloggers based on predictions?

## How to Install

    Run:
        conda install --file requirements.txt
    Run:
        pip install -U -r requirements.txt
    Run:
        conda install pytorch torchvision torchaudio cudatoolkit=11.0 -c pytorch

## To Refresh Data:
    
    1. Get fat tickers list: constants.FAT_TICKERS_PATH
        Aka: test_ticker_service.py: test_compose_ticker_list function
    2. Run scrape_tipranks.
        This will produce: constants.ANALYST_STOCK_PICKS_FROM_TICKER_PATH
    3. Run pipe_clean.py
        This will take output from #2, process and output to constants.TIP_RANKS_STOCK_DATA_PATH
    4. Run pip_agg.py
        This will output constants.TIP_RANKS_SCORED_FULL
    5. Run pipe_backtest.py
        This will show output

#Ideas

1. New col 'acc. score up til now on this ticker for this analyst.'
2. New col 'acc. score up til now on all tickers for this analyst.'
3. New col 'acc. score up til now on this ticker for all analysts.'
4. New col 'acc. score up til now on all tickers for all analysts.'