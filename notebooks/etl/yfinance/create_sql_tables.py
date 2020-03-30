import pymysql

from yitian.datasource import *

# input password from console
password = locals()['password']


# Set up cloud sql connections
connection = pymysql.connect(host=PRIVATE_HOST,
                             user=USER,
                             password=password,
                             db=DATABASE)


# CREATE TABLES IF NOT EXIST
with connection.cursor() as cursor:


    # Create NASDAQ ticker table
    sql = """
            CREATE TABLE IF NOT EXISTS nasdaq(
                symbol VARCHAR(10),
                name VARCHAR(75),
                marketcap VARCHAR(10),
                ipo_year INT,
                sector VARCHAR(25),
                industry VARCHAR(50),
                summary VARCHAR(100),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            PRIMARY KEY(symbol));
        """
    cursor.execute(sql)


    # Create NYSE ticker table
    sql = """
            CREATE TABLE IF NOT EXISTS nyse(
                symbol VARCHAR(10),
                name VARCHAR(75),
                marketcap VARCHAR(10),
                ipo_year INT,
                sector VARCHAR(25),
                industry VARCHAR(50),
                summary VARCHAR(100),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            PRIMARY KEY(symbol));
            """
    cursor.execute(sql)


    # Create NASDAQ Stock History Daily table
    sql = """
            CREATE TABLE IF NOT EXISTS nasdaq_daily(
                ticker VARCHAR(10),
                date DATE NOT NULL,
                open FLOAT NOT NULL,
                high FLOAT NOT NULL,
                low FLOAT NOT NULL,
                close FLOAT NOT NULL,
                volume FLOAT DEFAULT 0.0 NOT NULL,
                year INT NOT NULL,
                month INT NOT NULL,
                day INT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            PRIMARY KEY(ticker, date));
    """
    cursor.execute(sql)


    # Create NASDAQ Stock History Hourly table
    sql = """
            CREATE TABLE IF NOT EXISTS nasdaq_hourly(
                ticker VARCHAR(10),
                datetime TIMESTAMP NOT NULL,
                open FLOAT NOT NULL,
                high FLOAT NOT NULL,
                low FLOAT NOT NULL,
                close FLOAT NOT NULL,
                volume FLOAT DEFAULT 0.0 NOT NULL,
                year INT NOT NULL,
                month INT NOT NULL,
                day INT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            PRIMARY KEY(ticker, datetime));
        """
    cursor.execute(sql)


    # Create `Stock Actions (Dividends & Splits)` table
    sql = """
            CREATE TABLE IF NOT EXISTS stock_actions(
                ticker VARCHAR(10),
                date DATE NOT NULL,
                dividends FLOAT NOT NULL,
                splits FLOAT NOT NULL,
            PRIMARY KEY(ticker, date));
        """
    cursor.execute(sql)


    # Create `Major Holders` table
    sql = """
            CREATE TABLE IF NOT EXISTS stock_holders(
                ticker VARCHAR(10),
                date_updated DATE NOT NULL,
                insider_share_pct FLOAT NOT NULL,
                institution_share_pct FLOAT NOT NULL,
                institution_float_pct FLOAT NOT NULL,
                institution_number INT NOT NULL,
            PRIMARY KEY(ticker, date_updated));
            """
    cursor.execute(sql)


    # Create `Institutional Holders` table
    sql = """
            CREATE TABLE IF NOT EXISTS institutional_holders(
                ticker VARCHAR(10),
                holder VARCHAR(75) NOT NULL,
                shares BIGINT NOT NULL,
                date_reported DATE NOT NULL,
                out_pct FLOAT NOT NULL,
                value BIGINT NOT NULL,
            PRIMARY KEY(ticker, date_reported));
        """
    cursor.execute(sql)


    # Create `Stock Recommendations` table
    sql = """
            CREATE TABLE IF NOT EXISTS stock_recommendations(
                ticker VARCHAR(10),
                holder VARCHAR(75) NOT NULL,
                shares BIGINT NOT NULL,
                date_reported DATE NOT NULL,
                out_pct FLOAT NOT NULL,
                value BIGINT NOT NULL,
            PRIMARY KEY(ticker, date_reported));
        """
    cursor.execute(sql)


# connection is not autocommit by default. So you must commit to save your changes.
connection.commit()


# Close connection
connection.close()
