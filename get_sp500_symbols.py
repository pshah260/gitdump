from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from sqlalchemy import create_engine
import argparse
import psycopg2 as pg


def sp():

    r = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    soup = bs(r.content, "lxml")
    table = soup.find_all("table")[0]
    df = pd.read_html(str(table))[0]
    df = df[["Symbol"]]

    table2 = soup.find_all("table")[1]
    df2 = pd.read_html(str(table2))[0]

    df2.columns = df2.columns.droplevel(level=0)
    df2 = df2['Ticker']
    df2.columns = ["Add", "Remove"]

    df3 = df2[["Add"]]
    df3 = df3.dropna(axis=0, how='any')
    df3.columns = ["Symbol"]

    df4 = df2[["Remove"]]
    df4 = df4.dropna(axis=0, how='any')
    df4.columns = ["Symbol"]


    r = pd.merge(df, df3, on='Symbol', how='outer')
    s = pd.merge(r, df4, how='outer', indicator=True)
    s = s.loc[s._merge == 'left_only', ['Symbol']]

    engine = create_engine('postgresql://ai:ai@localhost:5432/stock')
    s.to_sql('sp500', engine)


def db_table_delete():
    conn_string = "host='localhost' dbname='stock' user='ai' password='ai'"
    conn = pg.connect(conn_string)
    cursor = conn.cursor()
    conn.set_isolation_level(0)
    query = "drop table \"sp500\";"
    try:
        cursor.execute(query)
    except:
        print "No table to be deleted"

def main():
    parser = argparse.ArgumentParser(description="Script creates tables for return")
    parser.add_argument('delete', help='If all tables needed to be deleted')
    delete = parser.parse_args().delete
    if delete:
        db_table_delete()
    else:
        print "No table to be deleted"
    sp()

if __name__ == "__main__":
    main()
