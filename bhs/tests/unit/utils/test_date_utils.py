from bhs.utils import date_utils


def test_parse_raw_tipranks_date():
    # Arrange
    date_str = "2021-02-02T06:00:00.000Z"

    # Act
    dt = date_utils.parse_tipranks_dt(date_str=date_str)

    # Assert
    print(dt)