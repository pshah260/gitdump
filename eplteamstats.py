from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine
import pandas as pd
import argparse
import psycopg2 as pg
import requests


def teams():
    engine = create_engine('postgresql://ai:ai@localhost:5432/soccer')
    query = "select * from \"epllinks\";"
    df = pd.read_sql_query(query,engine)
    for i in range(0,20):
        teamurl = df.URLS[i]
        teamn = df.Teams[i]
        r = requests.get(teamurl)
        soup = bs(r.content, "lxml")
        table = soup.find_all("table")[0]
        df2= pd.read_html(str(table))[0]
        table2 = soup.find_all("table")[1]
        df3 = pd.read_html(str(table2))[0]
        df4 = pd.merge(df2,df3, on='Name')
        engine = create_engine('postgresql://ai:ai@localhost:5432/soccer')
        df4.to_sql(teamn, engine)


def db_table_delete():
    conn_string = "host='localhost' dbname='soccer' user='ai' password='ai'"
    conn = pg.connect(conn_string)
    cursor = conn.cursor()
    conn.set_isolation_level(0)
    engine = create_engine('postgresql://ai:ai@localhost:5432/soccer')
    query = "select * from \"epllinks\";"
    df = pd.read_sql_query(query,engine)
    for i in range(0,20):
        team = df['Teams'][i]
        query = "drop table \"" + team  + "\";"
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
    teams()

if __name__ == "__main__":
    main()
