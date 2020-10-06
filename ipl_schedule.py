from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from sqlalchemy import create_engine
import argparse
import psycopg2 as pg


def sp():

    r = requests.get("https://indianexpress.com/article/sports/ipl/ipl-2020-full-schedule-fixtures-start-date-timings-venues-6583339/")
    soup = bs(r.content, "lxml")
    table = soup.find_all("table")[0]
    df = pd.read_html(str(table))[0]
    df.columns = ["Match No", "Match", "Date", "Time", "Location"]
    df = df[1:]
    df[['TeamA','TeamB']] = df.Match.str.split(" vs", expand=True)
    df.TeamB = df.TeamB.str.lstrip()
#    df = df.drop(["Match No", "Match", "Date", "Time", "Location"], axis=1)

    print df

    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
#    df.to_sql('schedule', engine)


def main():
    parser = argparse.ArgumentParser(description="Script creates tables for return")
    parser.add_argument('delete', help='If all tables needed to be deleted')
    sp()

if __name__ == "__main__":
    main()
