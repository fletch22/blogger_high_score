from bhs.utils import date_utils


def test_parse_raw_tipranks_date():
    # Arrange
    date_str = "Wed Aug 12 2020"

    # Act
    dt = date_utils.parse_tipranks_dt(date_str=date_str)

    # Assert
    print(dt)