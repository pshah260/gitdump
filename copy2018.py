from sqlalchemy import create_engine
import pandas as pd
import argparse
import psycopg2 as pg
from datetime import datetime
import numpy as np
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)

def batpoints():
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    runs_query = 'select * from "most-runs-18";'
    rundf = pd.read_sql_query(runs_query,engine)
    rundf = rundf.drop(['index', 'POS', 'NO', 'Mat', 'HS', 'Avg', 'BF'], axis=1)
    wickets_query = 'select * from "most-wickets-18";'
    wicketsdf = pd.read_sql_query(wickets_query,engine)
    wicketsdf = wicketsdf.drop(['index', 'POS', 'Ov', 'Mat', 'Runs', 'BBI', 'Avg', 'SR'], axis=1)
    points_query = 'select * from "points-18";'
    pointsdf = pd.read_sql_query(points_query,engine)
    pointsdf = pointsdf.drop(['index', 'POS', 'Pts', 'Wkts', 'Dots', '4s', '6s'], axis=1)
    m_query = 'select * from "maidens-18";'
    mdf = pd.read_sql_query(m_query,engine)
    mdf = mdf.drop(['index', 'POS', 'Inns', 'Ov', 'Mat', 'Runs', 'Wkts', 'Avg', 'Econ', 'SR', '4w', '5w'], axis=1)

    mergedf = rundf.merge(wicketsdf, left_on='PLAYER', right_on='PLAYER', how='outer')
    mergedf = mergedf.merge(pointsdf, left_on='PLAYER', right_on='PLAYER', how='outer')
    mergedf = mergedf.merge(mdf, left_on='PLAYER', right_on='PLAYER', how='outer')
    df = mergedf.fillna(0)

    df['TotalPTS'] = df['Runs']*1 + df['4s']*1 + df['6s']*2 + df['50']*8 + df['100']*16 + df['Wkts']*25 + df['4w']*8 + df['5w']*16 + df['Maid']*8 + df['Catches']*8 + df['Stumpings']*16 + df.apply(h, axis=1)*4 + df['Inns_y']*df.apply(f,axis=1) + df['Inns_x']*df.apply(g,axis=1)
    df['PTSPMAT'] = df['TotalPTS'] / df.apply(h, axis=1)
    df = df[~df.isin([np.nan, np.inf, -np.inf]).any(1)]
    df = df.sort_values(by=['PTSPMAT'], ascending=False)
    df.to_sql('2018', engine)

def f(x):
    if x['Econ'] == 0: return 0
    elif x['Econ'] < 4 and x['Econ'] != 0: return 6
    elif x['Econ'] >= 4 and x['Econ'] < 5: return 4
    elif x['Econ'] >= 5 and x['Econ'] <= 6: return 2 
    elif x['Econ'] >= 9 and x['Econ'] <= 10: return -2 
    elif x['Econ'] > 10 and x['Econ'] <= 11: return -4 
    elif x['Econ'] > 11: return -6 
    else: return 0

def g(x):
    if x['SR'] == 0: return 0
    elif x['SR'] < 50 and x['SR'] != 0: return -6
    elif x['SR'] >= 50 and x['SR'] < 60: return -4
    elif x['SR'] >= 60 and x['SR'] <= 70: return -2 
    else: return 0 


def h(x):
    if x['Inns_y'] > x['Inns_x']: return x['Inns_y']
    elif x['Inns_y'] <= x['Inns_x']: return x['Inns_x']
    else: return 0




def main():
    parser = argparse.ArgumentParser(description="Script creates tables for return")
    batpoints()


if __name__ == "__main__":
    main()
