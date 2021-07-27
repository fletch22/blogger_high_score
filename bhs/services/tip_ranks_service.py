import time
from pathlib import Path
from typing import List, Dict

import pandas as pd
from retry import retry
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from bhs.config import constants, logger_factory
from bhs.services import ticker_service

profile_path_str = "C:\\Users\\Chris\\AppData\\Local\\Mozilla\\Firefox\\Profiles\\53mnyz5y.selenium_blogger_high_score"
fp = webdriver.FirefoxProfile(profile_path_str)

logger = logger_factory.create_instance(__name__)


class AnalystTip:
    ticker = None
    analyst_rel_url = None
    analyst_name = None
    rank_raw = None
    org_name = None
    rating = None
    target_price = None
    tr_rating_roi = None
    rating_initiating = None
    dt_rating = None


def get_driver():
    return webdriver.Firefox(fp)


def find_all_data_on_page(driver, ticker) -> List[AnalystTip]:
    icon_str_xp = './/a[@data-link="expert" and contains(@class, "colorlink")]'
    condition = EC.presence_of_element_located((By.XPATH, icon_str_xp))
    _: WebElement = WebDriverWait(driver, 15).until(condition)

    row_xp = '//div[contains(@class,"rt-tr-group")]'
    rows = driver.find_elements_by_xpath(row_xp)

    logger.info(f"Num elements: {len(rows)}")

    all_at = []
    for ndx, div_el in enumerate(rows):
        at = AnalystTip()
        at.ticker = ticker

        xpath_name = './/a[@data-link="expert" and contains(@class, "colorlink")]'
        anchor_elems = div_el.find_elements_by_xpath(xpath_name)
        if len(anchor_elems) == 0:
            logger.info("Getting unknown analyst span...")
            xpath_name = './/span[contains(text(), "Unknown Analyst")]'
            unknown_els = div_el.find_elements_by_xpath(xpath_name)
            if len(unknown_els) == 0:
                logger.info("Getting named unrated analyst span...")
                xpath_name = './/span[@class="fontWeightsemibold"]'
                analyst_els = div_el.find_elements_by_xpath(xpath_name)
                if len(analyst_els) == 0:
                    continue
                at.analyst_name = analyst_els[0].text
                at.rank_raw = "unknown"
            else:
                at.analyst_name = "unknown"
                at.rank_raw = "unknown"
        else:
            a_el = anchor_elems[0]
            at.analyst_name = a_el.get_attribute("title")
            at.analyst_rel_url = a_el.get_attribute("href")

            span = div_el.find_element_by_xpath('.//div[contains(@class, "colorgray-1")]/span')

            width = span.get_attribute('style').split("width:")[1].split("%")[0]
            at.rank_raw = f"{(float(width) / 100) * 5:.2}"

        logger.info("Getting cells span...")
        org_xpath = './/div[contains(@class, "rt-tr")]/div[contains(@class, "rt-td") and contains(@class, "rt-left")]/span[@class="truncate"]'
        org_els = div_el.find_elements_by_xpath(org_xpath)
        if len(org_els) == 0:
            at.org_name = "not found"
        else:
            at.org_name = org_els[0].get_attribute("title")

        analyst_cells_xpath = './/div[contains(@class, "rt-tr")]/div[contains(@class, "rt-td") and contains(@class, "rt-left")]'
        analyst_cells = div_el.find_elements_by_xpath(analyst_cells_xpath)

        if len(analyst_cells) <= 2:
            continue

        tar_price_el = analyst_cells[2]
        logger.info("Getting target_price span...")
        span_els = tar_price_el.find_elements_by_xpath('.//span')

        if len(span_els) == 0:
            at.target_price = "unknown"
        else:
            at.target_price = span_els[0].get_attribute("data-value")

        rating_el = analyst_cells[3]
        logger.info("Getting rating span...")
        span_els = rating_el.find_elements_by_xpath('.//span[contains(@class, "textTransformuppercase")]')
        if len(span_els) == 0:
            at.rating = "unknown"
        else:
            at.rating = span_els[0].text

        tr_rating_roi_el = analyst_cells[4]
        span_els = tr_rating_roi_el.find_elements_by_xpath('.//div[contains(@class, "flexr_b")]')
        if len(span_els) == 0:
            at.tr_rating_roi = "unknown"
        else:
            at.tr_rating_roi = span_els[0].text.split("%")[0]

        rat_init_el = analyst_cells[5]
        logger.info("Getting rat_init span...")
        span_els = rat_init_el.find_elements_by_xpath('.//span[contains(@class, "colorblack")]')
        if len(span_els) == 0:
            at.rating_initiating = "unknown"
        else:
            at.rating_initiating = span_els[0].text

        dt_rat_el = analyst_cells[6]
        time_els = dt_rat_el.find_elements_by_xpath('.//time')
        if len(time_els) == 0:
            at.dt_rating = "unknown"
        else:
            at.dt_rating = time_els[0].get_attribute("datetime")

        logger.info(
            f"{ndx}: {at.analyst_name}; {at.analyst_rel_url}; {at.rank_raw}; {at.org_name}; {at.target_price}; {at.rating}; {at.tr_rating_roi}; {at.rating_initiating}; {at.dt_rating}")

        all_at.append(at)

    return all_at


@retry(tries=-1, delay=15, max_delay=60, backoff=1, jitter=1, logger=logger)
def call_tip_ranks(driver, url: str, ticker: str):
    driver.get(url)

    if driver.title.startswith("Page Not Found") or driver.title == "":
        return None

    more_to_show = click_show_more(driver)
    while more_to_show:
        more_to_show = click_show_more(driver)
        time.sleep(1)

    time.sleep(8)

    return find_all_data_on_page(driver, ticker=ticker)


def get_stock_tips(tickers: List[str], driver: WebDriver) -> Dict[str, List[AnalystTip]]:
    stock_tips = dict()
    for t in tickers:
        print(f"Getting ticker {t}")
        url = f"https://www.tipranks.com/stocks/{t}/forecast"

        tips = call_tip_ranks(driver=driver, url=url, ticker=t)
        if tips is None:
            continue
        else:
            stock_tips[t] = tips

    return stock_tips


def click_show_more(driver: WebDriver):
    more_to_show = True
    timeout = 1

    rule_1 = '//button/span[text()="Show More Ratings"]'
    try:
        logger.info("Getting show_more span 1...")
        condition = EC.presence_of_element_located((By.XPATH, rule_1))
        logger.info("Getting show_more span 2...")
        WebDriverWait(driver, timeout).until(condition).click()
        logger.info("Getting show_more span 3...")
    except TimeoutException as te:
        print(te)
        more_to_show = False

    return more_to_show


def login(driver: WebDriver) -> WebElement:
    url = "https://www.tipranks.com/sign-in?redirectTo=%2F"
    driver.get(url)

    # TODO: 2020-12-02: chris.flesche: Move to config
    # username = "fletch22.tester.1@gmail.com"
    # pwd = "U71Er%rRh53*"
    username = constants.tipranks_username
    pwd = constants.tipranks_password

    login_tb = '//input[@name="email" and @type="email"]'
    condition = EC.presence_of_element_located((By.XPATH, login_tb))
    user_elem: WebElement = WebDriverWait(driver, 15).until(condition)
    user_elem.send_keys(username)

    pwd_tb = '//input[@name="password" and @type="password"]'
    condition = EC.presence_of_element_located((By.XPATH, pwd_tb))
    pwd_elem: WebElement = WebDriverWait(driver, 15).until(condition)
    pwd_elem.send_keys(pwd)

    sign_in_bt = '//button[@class="client-templates-loginPage-styles__submitButton"]'
    condition = EC.presence_of_element_located((By.XPATH, sign_in_bt))
    sign_in_elem: WebElement = WebDriverWait(driver, 15).until(condition)
    sign_in_elem.click()


def scrape_tipranks(tickers: List[str], output_path: Path):
    driver = get_driver()

    login(driver=driver)
    time.sleep(1)

    stock_tips = None
    try:
        stock_tips = get_stock_tips(driver=driver, tickers=tickers)
    except Exception as e:
        print(e)

    if stock_tips is not None:
        all_tips = []
        for ticker, tips in stock_tips.items():
            print(f"\nTicker: {ticker}\n")

            for at in tips:
                all_tips.append(at.__dict__)

        df = pd.DataFrame(all_tips)
        if output_path:
            df.to_parquet(output_path)

    return stock_tips


def get_stock_data_for_ranks(df: pd.DataFrame, num_days_in_future: int = 1):
    ttd = extract_tipranks_ticker_dates(df)
    return ticker_service.get_ticker_on_dates(ttd, num_days_in_future=num_days_in_future)


def extract_tipranks_ticker_dates(df: pd.DataFrame):
    df_g_stocks = df.groupby(by=["ticker"])

    stock_days = {}
    for group_name, df_group in df_g_stocks:
        ticker = group_name
        dates = df_group["dt_rating"].values.tolist()
        stock_days[ticker] = dates

    return stock_days


if __name__ == '__main__':
    df = ticker_service.get_fat_tickers()
    tickers = df["ticker"].to_list()
    file_path = constants.ANALYST_STOCK_PICKS_FROM_TICKER_PATH

    tickers = sorted(tickers)

    # Act
    stock_tips = scrape_tipranks(tickers=tickers, output_path=file_path)

    for ticker, tips in stock_tips.items():
        print(f"Ticker: {ticker}")