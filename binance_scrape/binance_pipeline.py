import sqlite3
import pandas as pd

pd.set_option('display.max_columns', 9)
pd.set_option('display.width', 360)


class BinancePipeline:
    def __init__(self):
        self.con = sqlite3.connect('binance.db')
        self.cur = self.con.cursor()

    def create_table_usd(self) -> None:
        """creates a new table if one doesn't already exist for the USD data"""

        self.cur.execute("""CREATE TABLE IF NOT EXISTS usd_coins(
                                row_id INTEGER PRIMARY KEY,
                                date TEXT,
                                pair TEXT,
                                coin TEXT,
                                price REAL,
                                daily_change TEXT,
                                daily_high REAL,
                                daily_low REAL,
                                daily_volume REAL)""")

    def process_item_usd(self, item: dict) -> dict:
        """processes and loads a row of data into USD table"""

        self.cur.execute("""INSERT OR IGNORE INTO usd_coins VALUES (?,?,?,?,?,?,?,?,?)""",
                         (None, item['date'], item['pair'], item['coin'], item['price'], item['24h_change'],
                          item['24h_high'], item['24h_low'], item['24h_volume']))
        self.con.commit()
        return item

    def create_table_usdt(self) -> None:
        """creates a new table if one doesn't already exist for the USDT data"""

        self.cur.execute("""CREATE TABLE IF NOT EXISTS usdt_coins(
                                row_id INTEGER PRIMARY KEY,
                                date TEXT,
                                pair TEXT,
                                coin TEXT,
                                price REAL,
                                daily_change TEXT,
                                daily_high REAL,
                                daily_low REAL,
                                daily_volume REAL)""")

    def process_item_usdt(self, item: dict) -> dict:
        """processes and loads a row of data into USDT table"""

        self.cur.execute("""INSERT OR IGNORE INTO usdt_coins VALUES (?,?,?,?,?,?,?,?,?)""",
                         (None, item['date'], item['pair'], item['coin'], item['price'], item['24h_change'],
                          item['24h_high'], item['24h_low'], item['24h_volume']))
        self.con.commit()
        return item

    def create_table_btc(self) -> None:
        """creates a new table if one doesn't already exist for the BTC data"""

        self.cur.execute("""CREATE TABLE IF NOT EXISTS btc_coins(
                                row_id INTEGER PRIMARY KEY,
                                date TEXT,
                                pair TEXT,
                                coin TEXT,
                                price REAL,
                                daily_change TEXT,
                                daily_high REAL,
                                daily_low REAL,
                                daily_volume REAL)""")

    def process_item_btc(self, item: dict) -> dict:
        """processes and loads a row of data into BTC table"""

        self.cur.execute("""INSERT OR IGNORE INTO btc_coins VALUES (?,?,?,?,?,?,?,?,?)""",
                         (None, item['date'], item['pair'], item['coin'], item['price'], item['24h_change'],
                          item['24h_high'], item['24h_low'], item['24h_volume']))
        self.con.commit()
        return item

    def get_all(self, table: str) -> pd.DataFrame:
        """SELECT * FROM table query"""

        sqlite_select_query = f"""SELECT * FROM {table}"""
        records = pd.read_sql_query(sqlite_select_query, self.con, index_col='row_id')
        return records

    def close(self):
        """close database connection"""

        self.con.close()

