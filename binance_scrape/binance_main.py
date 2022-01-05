import pandas as pd
from playwright.sync_api import sync_playwright
from binance_pipeline import BinancePipeline
from binance_emailer import EmailSender
from bs4 import BeautifulSoup
import datetime as dt
import logging
import time
import os

URL = "https://www.binance.us/en/markets"
TODAY = dt.date.today()
logging.basicConfig(filename='scraper.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


def run(p) -> None:
    """begins running playwright and takes us to the specified web page.
    Initiates chart selection process using playwright, which starts the ETL process"""

    bp = BinancePipeline()
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(user_agent=os.environ.get('USER_AGENT'))
    page = context.new_page()
    page.goto(URL)
    time.sleep(2)
    select_charts(page, bp)
    time.sleep(1)
    context.close()
    browser.close()
    bp.close()


def select_charts(page, bp: BinancePipeline) -> None:
    """uses playwright to select each tab on the main site,
    since selecting each tab does not change the url. Finally,
    starts the ETL process for each tab"""

    frames = []
    tabs = ['USD', 'USDⓈ', 'BTC']
    for tab in tabs:
        page.click(f'text={tab} Markets')  # move to specified tab
        time.sleep(1)
        records = run_etl_process(page, bp, tab)  # run etl_process function
        print(records)
        frames.append(records)
        time.sleep(1)

    send_dfs_in_email(pd.concat(frames))


def extract(page) -> list:
    """runs the extraction phase of ETL process.
    Uses BeautifulSoup to extract data from the inner html provided by playwright"""

    rows = page.query_selector_all('div.column-container')
    soups = []
    for row in rows:
        soup = BeautifulSoup(row.inner_html(), 'html.parser')
        soups.append(soup)
    return soups


def transform(soups: list) -> list:
    """runs the transform phase of the ETL process.
    takes a list of BeautifulSoup objects as a parameter, which was created in extraction phase,
    returns a list of dictionaries containing transformed row data"""

    soup_data = []
    for soup in soups:
        pair = soup.find('div', {'class': 'sc-1f6hdl7-11 DZeuB'}).text.strip()
        coin = soup.find('span', {'style': 'max-width: 95px; text-overflow: ellipsis; overflow: hidden;'}).text.strip()
        price = soup.find('div', {'style': 'text-align: left; line-height: 18px;'}).text.strip().replace(',', '')
        try:
            price = float(price)
        except ValueError:
            price = float(price.split('$')[0])
        change_24h = soup.find('div', {'aria-colindex': '5'}).text.strip()
        high_24h = float(soup.find('div', {'aria-colindex': '6'}).text.strip().replace(',', ''))
        low_24h = float(soup.find('div', {'aria-colindex': '7'}).text.strip().replace(',', ''))
        volume_24h = float(soup.find('div', {'aria-colindex': '9'}).text.strip().replace(',', ''))

        my_dict = {
            'date': TODAY,
            'pair': pair,
            'coin': coin,
            'price': price,
            '24h_change': change_24h,
            '24h_high': high_24h,
            '24h_low': low_24h,
            '24h_volume': volume_24h
        }

        soup_data.append(my_dict)

    return soup_data


def load(soup_data: list, bp: BinancePipeline, tab: str) -> str:
    """runs the load phase of the ETL process.
    loads data into local sqlite3 database"""

    table = ''
    if tab == 'USD':
        table = 'usd_coins'
        for data in soup_data:
            bp.create_table_usd()
            bp.process_item_usd(data)
    elif tab == 'USDⓈ':
        table = 'usdt_coins'
        for data in soup_data:
            bp.create_table_usdt()
            bp.process_item_usdt(data)
    elif tab == 'BTC':
        table = 'btc_coins'
        for data in soup_data:
            bp.create_table_btc()
            bp.process_item_btc(data)
    return table


def run_etl_process(page, bp: BinancePipeline, tab: str) -> pd.DataFrame:
    """calls the functions related to each phase of the ETL process,
    then runs a SELECT query on the specified table to show successful data entry into sqlite3 database"""

    soups = extract(page)  # extract html text
    soup_data = transform(soups)  # transform data into dictionary format
    table = load(soup_data, bp, tab)  # load dict data into sqlite3 table
    records = bp.get_all(table)  # run SELECT query
    return records


def send_dfs_in_email(df):
    e = EmailSender()
    e.run_server()
    e.get_dataframe(df)
    e.get_contacts()
    e.read_template()
    e.compose_email()
    e.send_email()
    e.close_server()


if __name__ == '__main__':
    with sync_playwright() as playwright:
        run(playwright)

