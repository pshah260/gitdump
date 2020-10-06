from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from sqlalchemy import create_engine
import argparse
import psycopg2 as pg
import re

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
