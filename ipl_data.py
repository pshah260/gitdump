from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from sqlalchemy import create_engine
import argparse
import psycopg2 as pg


def sp():

    r = requests.get("https://www.iplt20.com/stats/2020/most-wickets")
    soup = bs(r.content, "lxml")
    table = soup.find_all("table")[0]
    df = pd.read_html(str(table))[0]
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    df.to_sql('most-wickets-20', engine)

    r = requests.get("https://www.iplt20.com/stats/2020/most-runs")
    soup = bs(r.content, "lxml")
    table = soup.find_all("table")[0]
    df = pd.read_html(str(table))[0]
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    df.to_sql('most-runs-20', engine)

    r = requests.get("https://www.iplt20.com/stats/2020/player-points")
    soup = bs(r.content, "lxml")
    table = soup.find_all("table")[0]
    df = pd.read_html(str(table))[0]
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    df.to_sql('points-20', engine)

    r = requests.get("https://www.iplt20.com/stats/2020/most-maidens")
    soup = bs(r.content, "lxml")
    table = soup.find_all("table")[0]
    df = pd.read_html(str(table))[0]
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    df.to_sql('maidens-20', engine)


def db_table_delete():
    conn_string = "host='localhost' dbname='ipl' user='ai' password='ai'"
    conn = pg.connect(conn_string)
    cursor = conn.cursor()
    conn.set_isolation_level(0)
    query = "drop table \"most-wickets-20\";"
    try:
        cursor.execute(query)
    except:
        print "No table to be deleted"
    query = "drop table \"most-runs-20\";"
    try:
        cursor.execute(query)
    except:
        print "No table to be deleted"
    query = "drop table \"points-20\";"
    try:
        cursor.execute(query)
    except:
        print "No table to be deleted"
    query = "drop table \"maidens-20\";"
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
