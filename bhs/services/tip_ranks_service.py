import time
from typing import List

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

profile_path_str = "C:\\Users\\Chris\\AppData\\Local\\Mozilla\\Firefox\\Profiles\\53mnyz5y.selenium_blogger_high_score"
fp = webdriver.FirefoxProfile(profile_path_str)
driver = webdriver.Firefox(fp)


class AnalystTip:
    analyst_rel_url = None
    analyst_name = None
    rank_raw = None
    org_name = None



def get_analyst_tips():
    x_path = '//img[@class="client-components-ReactTableWrapper-cells__expertImg"]'

    img_elems = driver.find_elements_by_xpath(x_path)

    print(f"Found {len(img_elems)} matching web elements.")

    all_analysts = []
    for ndx, i in enumerate(img_elems):

        parent = i.find_element_by_xpath("..")

        if parent.tag_name == "a":
            at = AnalystTip()
            at.analyst_rel_url = parent.get_attribute("href")

            ancestor = parent.find_element_by_xpath("..") \
                .find_element_by_xpath("..") \
                .find_element_by_xpath("..") \
                .find_element_by_xpath("..") \
                .find_element_by_xpath("..")

            xpath_name = './/span[@itemprop="name"]'
            name = ancestor.find_element_by_xpath(xpath_name)
            at.analyst_name = name.text

            xpath_rank = './/span[@class="client-NewComponents-SmartCombos-rank-styles__rankFilled"]/span'
            rank_raw = ancestor.find_element_by_xpath(xpath_rank)
            at.rank_raw = rank_raw.get_attribute('innerHTML')

            xpath_org_name = './/span[@class="client-components-ReactTableWrapper-styles__textEllipsis"]/span'
            org_name = ancestor.find_element_by_xpath(xpath_org_name)
            at.org_name = org_name.text

            all_analysts.append(at)

    return all_analysts


def get_stock_tips() -> List[AnalystTip]:
    url = "https://www.tipranks.com/stocks/fis/forecast"
    driver.get(url)

    # elem = driver.find_elements_by_class_name("tipranks-icon client-components-ReactTableWrapper-component__showMoreIcon")
    # rule_1 = "//*[@class='tipranks-icon' and @class='client-components-ReactTableWrapper-component__showMoreIcon']"
    # rule_1 = "//*[@class='client-components-ReactTableWrapper-component__showMoreIcon']"

    more_to_show = click_show_more()
    while more_to_show:
        more_to_show = click_show_more()
        time.sleep(1)

    return get_analyst_tips()


def click_show_more():
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


def login() -> WebElement:
    url = "https://www.tipranks.com/sign-in?redirectTo=%2F"
    driver.get(url)

    username = "fletch22.tester.1@gmail.com"
    pwd = "U71Er%rRh53*"

    # elem = driver.find_elements_by_class_name("tipranks-icon client-components-ReactTableWrapper-component__showMoreIcon")
    # rule_1 = "//*[@class='tipranks-icon' and @class='client-components-ReactTableWrapper-component__showMoreIcon']"
    # rule_1 = "//*[@class='client-components-ReactTableWrapper-component__showMoreIcon']"
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

    return driver
