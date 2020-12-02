import time

from bhs.services import tip_ranks_service


def test_get_stock_tips():
    # Arrange
    # Act
    elem, driver = tip_ranks_service.get_stock_tips()

    elem.click()

    # Assert
    print(str(elem))


def test_login():
    # Arrange
    # Act
    driver = tip_ranks_service.login()

    # Assert
    # print(str(elem))


def test_login_and_get_stocks():
    # Arrange
    # Act
    driver = tip_ranks_service.login()
    time.sleep(1)
    tips = tip_ranks_service.get_stock_tips()

    for t in tips:
        print(t.analyst_rel_url)
        print(t.analyst_name)
        print(t.rank_raw)
        print(t.org_name)
        print()

    # Assert
    assert(len(tips) > 0)
