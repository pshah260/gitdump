from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from sqlalchemy import create_engine
import argparse
import psycopg2 as pg


def sp():
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    query = 'select * from "schedule"'
    df = pd.read_sql_query(query,engine)
    df['CSK'] = df.apply(f, axis=1)
    df['DC'] = df.apply(g, axis=1)
    df['KXI'] = df.apply(h, axis=1)
    df['KKR'] = df.apply(i, axis=1)
    df['MI'] = df.apply(j, axis=1)
    df['RR'] = df.apply(k, axis=1)
    df['RCB'] = df.apply(l, axis=1)
    df['SRH'] = df.apply(m, axis=1)

    df['CSKS'] = df.CSK.rolling(4).sum()
    df['DCS'] = df.DC.rolling(4).sum()
    df['KXIS'] = df.KXI.rolling(4).sum()
    df['KKRS'] = df.KKR.rolling(4).sum()
    df['MIS'] = df.MI.rolling(4).sum()
    df['RRS'] = df.RR.rolling(4).sum()
    df['RCBS'] = df.RCB.rolling(4).sum()
    df['SRHS'] = df.SRH.rolling(4).sum()

    df['CSKS'] = df.CSKS.shift(periods=-3)
    df['DCS'] = df.DCS.shift(periods=-3)
    df['KXIS'] = df.KXIS.shift(periods=-3)
    df['KKRS'] = df.KKRS.shift(periods=-3)
    df['MIS'] = df.MIS.shift(periods=-3)
    df['RRS'] = df.RRS.shift(periods=-3)
    df['RCBS'] = df.RCBS.shift(periods=-3)
    df['SRHS'] = df.SRHS.shift(periods=-3)

    df = df.drop(["CSK","DC", "KXI", "KKR", "MI", "RR", "RCB", "SRH"],axis=1)
    df = df.fillna(1)
    df = df.drop(["index"], axis=1)
    df['sum'] = df.sum(axis=1)

    print df
#    df.to_sql("teamschedule", engine)


def f(x):
    if x['TeamA'] == 'CSK' or x['TeamB'] == 'CSK': return 1
    else: return 0

def g(x):
    if x['TeamA'] == 'DC' or x['TeamB'] == 'DC': return 1
    else: return 0

def h(x):
    if x['TeamA'] == 'KXIP' or x['TeamB'] == 'KXIP': return 1
    else: return 0

def i(x):
    if x['TeamA'] == 'KKR' or x['TeamB'] == 'KKR': return 1
    else: return 0

def j(x):
    if x['TeamA'] == 'MI' or x['TeamB'] == 'MI': return 1
    else: return 0

def k(x):
    if x['TeamA'] == 'RR' or x['TeamB'] == 'RR': return 1
    else: return 0

def l(x):
    if x['TeamA'] == 'RCB' or x['TeamB'] == 'RCB': return 1
    else: return 0

def m(x):
    if x['TeamA'] == 'SRH' or x['TeamB'] == 'SRH': return 1
    else: return 0

def main():
    parser = argparse.ArgumentParser(description="Script creates tables for return")
    sp()

if __name__ == "__main__":
    main()
