import sqlite3

from .config import Config
from .logger import Logger

class Database:

    def __init__(self, logger: Logger, config: Config, uri="data/database.db"):
        self.logger = logger
        self.config = config
        self.uri = uri
        self.connect_database()
        self.create_tables()
        self.initialize_user()
        
        
    def insert_update_table(self, query, arguments):
        cur = self.conn.cursor()
        self.logger.debug("Insert on table")
        cur.execute(query, arguments)
        self.conn.commit()
        
    def select_table(self, query, arguments):
        cur = self.conn.cursor()
        self.logger.debug("Select on table")
        cur.execute(query, arguments)
        self.conn.commit()
        return cur.fetchall()

    def connect_database(self):
        self.conn = sqlite3.connect(self.uri)
        self.logger.info("Opened database successfully")
        
    def create_tables(self):
        cur = self.conn.cursor()
        self.logger.debug("Creating tables")

        cur.execute(
            """CREATE TABLE IF NOT EXISTS USER(
                id integer PRIMARY KEY AUTOINCREMENT,
                name text NOT NULL,
                telegram_id integer,
                status text
                );""")

        cur.execute(
            """CREATE TABLE IF NOT EXISTS PAYMENT_HISTORIC(
                id integer PRIMARY KEY AUTOINCREMENT,
                user_id integer REFERENCES USER(id),
                total_btc real NOT NULL,
                paid text NOT NULL,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                );""")

        cur.execute(
            """CREATE TABLE IF NOT EXISTS TRADE_HISTORIC(
                id integer PRIMARY KEY AUTOINCREMENT,
                user_id integer REFERENCES USER(id),
                close real NOT NULL,
                side text NOT NULL,
                btc_amount real NOT NULL,
                second_coin text NOT NULL,
                second_coin_amount real NOT NULL,
                close_btc_to_brl real NOT NULL,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                );""")

        cur.execute(
            """CREATE TABLE IF NOT EXISTS ASSET_BALANCE(
                id integer PRIMARY KEY AUTOINCREMENT,
                user_id integer REFERENCES USER(id),
                btc real NOT NULL,
                usd real NOT NULL,
                brl real NOT NULL,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                );""")

        cur.execute(
            """CREATE TABLE IF NOT EXISTS PROFIT(
                id integer PRIMARY KEY AUTOINCREMENT,
                user_id integer REFERENCES USER(id),
                total_btc real NOT NULL,
                total_usd real NOT NULL,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                );""")

        cur.execute(
            """CREATE TABLE IF NOT EXISTS BASE_PRICE(
                id integer PRIMARY KEY AUTOINCREMENT,
                user_id integer REFERENCES USER(id),
                symbol text NOT NULL,
                ticker_price real NOT NULL,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                );""")
        
        self.conn.commit()

    def initialize_user(self):
        
        rows = self.select_table("SELECT * FROM USER WHERE name=?", (self.config.USER,))
        #Create user if do not exist
        if rows:
            self.logger.debug("User already created")
            self.insert_update_table("UPDATE USER SET status = ? WHERE name = ?", ("Active", self.config.USER))
        else:
            self.insert_update_table("INSERT INTO USER (name, telegram_id, status) VALUES (?,?,?)", (self.config.USER,self.config.TELEGRAM_ID,"Active"))

        rows = self.select_table("SELECT * FROM USER WHERE name=?", (self.config.USER,))
        self.logger.debug(rows)
        self.config.setUserID(rows[0][0])

        rows = self.select_table("SELECT * FROM PAYMENT_HISTORIC WHERE user_id=?", (self.config.USER_ID,))
        self.logger.debug(rows)
        if not rows:
            self.insert_update_table("INSERT INTO PAYMENT_HISTORIC (user_id, total_btc, paid) VALUES (?,?,?)", (self.config.USER_ID,0,"Paid"))
        else :
            self.logger.debug("User already registered in PAYMENT_HISTORIC")
            
        rows = self.select_table("SELECT * FROM PROFIT WHERE user_id=?", (self.config.USER_ID,))
        self.logger.debug(rows)
        if not rows:
            self.insert_update_table("INSERT INTO PROFIT (user_id, total_btc, total_usd) VALUES (?,?,?)", (self.config.USER_ID,0,0))
        else :
            self.logger.debug("User already registered in PROFIT")

            
    def set_ticker_prices(self, ticker_prices):
        for supported_symbol in self.config.SUPPORTED_SYMBOL_LIST:
            rows = self.select_table("SELECT * FROM BASE_PRICE WHERE user_id=? AND symbol=?", (self.config.USER_ID, supported_symbol))
            if not rows:
                self.insert_update_table("INSERT INTO BASE_PRICE (user_id, symbol, ticker_price) VALUES (?,?,?)", 
                (self.config.USER_ID,supported_symbol,next(ticker_price for ticker_price in ticker_prices if ticker_price["symbol"] == supported_symbol)["price"]))

    def disable_bot(self):
        self.insert_update_table("UPDATE USER SET status = ? WHERE id = ?", ("Inactive", self.config.USER_ID))