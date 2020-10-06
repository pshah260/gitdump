from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from sqlalchemy import create_engine
import argparse
import psycopg2 as pg
import re
import numpy as np

def sp():

    r = requests.get("https://www.espncricinfo.com/scores/series/8048/season/2020/indian-premier-league?view=results")
    soup = bs(r.content, "lxml")
    section = soup.find_all("section")[0]
    a = section.find_all("a", href=True)
    for data in a:
        if data['data-hover'] == 'Scorecard':
            r2 = requests.get("https://www.espncricinfo.com" + data['href'])
            soup2 = bs(r2.content, "lxml")
            t1 = soup2.find_all("table")[0]
            a = t1.find_all("a", href=True, title=True)
            t1 = str(t1)
            t1 = t1.replace("\xc2", "")
            t1 = t1.replace("\xa0", "")
            t1 = t1.replace("\xe2", "")
            t1 = t1.replace("\x80", "")
            t1 = re.sub('[(c)]', '', t1)
            df = pd.read_html(t1)[0]
            df = df[0:-4]
            df = df.dropna()
            for n in a:
                s = n.text
                st = re.sub('[(c)\xc2\xa0\xe2\x80]', '', s)
                print re.search('of\s(\w+\s\w+)', n["title"]).group(1), st
        else:
            continue

#    df = pd.read_html(str(table))[0]
#    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
#    df.to_sql('most-wickets-20', engine)

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


def main():
    parser = argparse.ArgumentParser(description="Script creates tables for return")
#    parser.add_argument('delete', help='If all tables needed to be deleted')
#    delete = parser.parse_args().delete
#    if delete:
#        db_table_delete()
#    else:
#        print "No table to be deleted"
    sp()


if __name__ == "__main__":
    main()
