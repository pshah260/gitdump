from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from sqlalchemy import create_engine
import argparse
import psycopg2 as pg


def sp():

    r = requests.get("https://www.iplt20.com/stats/2019/most-maidens")
    soup = bs(r.content, "lxml")
    table = soup.find_all("table")[0]
    df = pd.read_html(str(table))[0]
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    df.to_sql('maidens-19', engine)

    r = requests.get("https://www.iplt20.com/stats/2018/most-maidens")
    soup = bs(r.content, "lxml")
    table = soup.find_all("table")[0]
    df = pd.read_html(str(table))[0]
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    df.to_sql('maidens-18', engine)

    r = requests.get("https://www.iplt20.com/stats/2017/most-maidens")
    soup = bs(r.content, "lxml")
    table = soup.find_all("table")[0]
    df = pd.read_html(str(table))[0]
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    df.to_sql('maidens-17', engine)

    r = requests.get("https://www.iplt20.com/stats/2016/most-maidens")
    soup = bs(r.content, "lxml")
    table = soup.find_all("table")[0]
    df = pd.read_html(str(table))[0]
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    df.to_sql('maidens-16', engine)

    r = requests.get("https://www.iplt20.com/stats/2015/most-maidens")
    soup = bs(r.content, "lxml")
    table = soup.find_all("table")[0]
    df = pd.read_html(str(table))[0]
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    df.to_sql('maidens-15', engine)


def main():
    parser = argparse.ArgumentParser(description="Script creates tables for return")
    parser.add_argument('delete', help='If all tables needed to be deleted')
    sp()

if __name__ == "__main__":
    main()
