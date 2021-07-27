from bhs.pipeline.tipranks import pipe_backtest


def test_backtest():
    pipe_backtest.backtest()

    # source_path = constants.TIP_RANKS_SCORED_FULL
    # df = pd.read_parquet(source_path)
    # print(df["rating_score"].mean(0))


def test_date_bar_chart():
    raw_dates = ['2020-01-01', '2020-01-08', '2020-02-01', '2020-03-01']

    pipe_backtest.chart_month_counts(raw_dates)


def test_get_ranges_months():
    # Arrange
    start_std_dt = "2021-01-01"
    end_std_dt = "2022-01-01"
    # Act
    date_ranges = pipe_backtest.get_ranges_months(start_std_dt=start_std_dt,
                                                  end_std_dt=end_std_dt)

    # Assert
    print(date_ranges)
    print(len(date_ranges))