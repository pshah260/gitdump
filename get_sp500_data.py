import requests
import pandas as pd
from sqlalchemy import create_engine
import psycopg2 as pg
import time
import argparse

def db_table_delete():
    stocks = get_meta()
    for code in stocks["Symbol"]:
        meta_data = code
        conn_string = "host='localhost' dbname='sp500' user='ai' password='ai'"
        conn = pg.connect(conn_string)
        cursor = conn.cursor()
        conn.set_isolation_level(0)
        query = "drop table \"" + meta_data + "\";"
        try:
            cursor.execute(query)
        except:
            print "No table to be deleted"

def insert_table():
    stocks = get_meta()
    engine = create_engine('postgresql://ai:ai@localhost:5432/sp500')

    for code in stocks["Symbol"]:

        try:
            test_query = "select * from \"" + code + "\";"
            df = pd.read_sql_query(test_query,engine)
	except:
            function = "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&"
            symbol = "symbol=" + code + "&"
            apikey = "apikey="
            query = function + symbol + apikey
            time.sleep(20)

            try:
                rawdata = requests.get(query).json()
                data = rawdata["Weekly Adjusted Time Series"]
                df = pd.DataFrame.from_dict(data, orient='index')
                df.columns = ['low', 'open', 'high', 'close', 'volume', 'adj_close', 'div']
                df.to_sql(code, engine)
	        print "Moved to DB", code
            except:
                print "Unable to get data ", code
                continue

def get_meta():
    engine = create_engine('postgresql://ai:ai@localhost:5432/stock')
    query = "select \"Symbol\" from \"sp500\";"
    df = pd.read_sql_query(query,engine)
    return df

def main():
    parser = argparse.ArgumentParser(description="Script downloads data")
    parser.add_argument('delete', help='If all tables needed to be deleted')
    delete = parser.parse_args().delete
#    if delete:
#        db_table_delete()
#    else:
#        print "No table to be deleted"
    insert_table()

if __name__ == "__main__":
    main()

