import time
from pathlib import Path
from typing import List, Dict

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

profile_path_str = "C:\\Users\\Chris\\AppData\\Local\\Mozilla\\Firefox\\Profiles\\53mnyz5y.selenium_blogger_high_score"
fp = webdriver.FirefoxProfile(profile_path_str)


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


def get_analyst_tips(driver: WebDriver, ticker: str):
    x_path = '//img[@class="client-components-ReactTableWrapper-cells__expertImg"]'

    img_elems = driver.find_elements_by_xpath(x_path)

    print(f"Found {len(img_elems)} matching web elements.")

    all_analysts = []
    for ndx, i in enumerate(img_elems):

        parent = i.find_element_by_xpath("..")

        if parent.tag_name == "a":
            at = AnalystTip()
            at.ticker = ticker
            at.analyst_rel_url = parent.get_attribute("href")

            ancestor = parent.find_element_by_xpath("..") \
                .find_element_by_xpath("..") \
                .find_element_by_xpath("..") \
                .find_element_by_xpath("..") \
                .find_element_by_xpath("..")

            xpath_name = './/span[@itemprop="name"]'
            at.analyst_name = ancestor.find_element_by_xpath(xpath_name).text

            xpath_rank = './/span[@class="client-NewComponents-SmartCombos-rank-styles__rankFilled"]/span'
            at.rank_raw = ancestor.find_element_by_xpath(xpath_rank).get_attribute('innerHTML')

            xpath_rate = './/p[@class="client-components-ReactTableWrapper-cells__rate"]'
            at.rating = ancestor.find_element_by_xpath(xpath_rate).get_attribute('innerHTML')

            try:
                xpath_tar_price = './/span[@class="client-components-MoneyAnimation-styles__moneyCell"]'
                at.target_price = ancestor.find_element_by_xpath(xpath_tar_price).get_attribute(
                    'innerHTML')
            except NoSuchElementException as nsee:
                at.target_price = None

            try:
                xpath_tr_rating_roi = './/div[contains(@class, "client-components-ReactTableWrapper-cells__PriceCompareActionCell")]/p/strong'
                at.tr_rating_roi = ancestor.find_element_by_xpath(
                    xpath_tr_rating_roi).get_attribute('innerHTML')
            except NoSuchElementException as nsee:
                at.tr_rating_roi = None

            xpath_te = './/span[@class="client-components-ReactTableWrapper-styles__textEllipsis"]/span'
            elems = ancestor.find_elements_by_xpath(xpath_te)

            at.org_name = elems[0].get_attribute('innerHTML')
            at.rating_initiating = elems[1].get_attribute('innerHTML')
            at.dt_rating = elems[2].find_element_by_xpath("..").get_attribute('title')

            all_analysts.append(at)

    return all_analysts


def get_stock_tips(tickers: List[str], driver: WebDriver) -> Dict[str, List[AnalystTip]]:
    stock_tips = dict()
    for t in tickers:
        url = f"https://www.tipranks.com/stocks/{t}/forecast"
        driver.get(url)

        if driver.title.startswith("Page Not Found"):
            continue

        more_to_show = click_show_more(driver)
        while more_to_show:
            more_to_show = click_show_more(driver)
            time.sleep(1)

        ticker_tips = get_analyst_tips(driver, ticker=t)
        stock_tips[t] = ticker_tips

    return stock_tips


def click_show_more(driver: WebDriver):
    more_to_show = True
    timeout = 1
    rule_1 = '//button[text()="Show More Ratings"]'
    condition = EC.presence_of_element_located((By.XPATH, rule_1))
    try:
        show_mo_elem = WebDriverWait(driver, timeout).until(condition)
        show_mo_elem.click()
    except TimeoutException as te:
        print(te)
        more_to_show = False

    return more_to_show


def login(driver: WebDriver) -> WebElement:
    url = "https://www.tipranks.com/sign-in?redirectTo=%2F"
    driver.get(url)

    # TODO: 2020-12-02: chris.flesche: Move to config
    username = "fletch22.tester.1@gmail.com"
    pwd = "U71Er%rRh53*"

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


def scrap_tipranks(tickers: List[str], output_path: Path):
    driver = get_driver()

    login(driver=driver)
    time.sleep(1)

    stock_tips = get_stock_tips(driver=driver, tickers=tickers)

    all_tips = []
    for ticker, tips in stock_tips.items():
        print(f"\nTicker: {ticker}\n")

        for at in tips:
            all_tips.append(at.__dict__)

    df = pd.DataFrame(all_tips)
    df.to_parquet(output_path)

    return stock_tips
