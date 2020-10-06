from sqlalchemy import create_engine
import pandas as pd
import argparse
import psycopg2 as pg
from datetime import datetime
import numpy as np
from itertools import combinations

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)

def p2019():
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    runs_query = 'select * from "most-runs-19";'
    rundf = pd.read_sql_query(runs_query,engine)
    rundf = rundf.drop(['index', 'POS', 'NO', 'Mat', 'HS', 'Avg', 'BF'], axis=1)
    rundf.PLAYER = rundf.PLAYER.str.replace('  ', ' ')

    wickets_query = 'select * from "most-wickets-19";'
    wicketsdf = pd.read_sql_query(wickets_query,engine)
    wicketsdf = wicketsdf.drop(['index', 'POS', 'Ov', 'Mat', 'Runs', 'BBI', 'Avg', 'SR'], axis=1)
    wicketsdf.PLAYER = wicketsdf.PLAYER.str.replace('  ', ' ')

    points_query = 'select * from "points-19";'
    pointsdf = pd.read_sql_query(points_query,engine)
    pointsdf = pointsdf.drop(['index', 'POS', 'Pts', 'Wkts', 'Dots', '4s', '6s'], axis=1)
    pointsdf.PLAYER = pointsdf.PLAYER.str.replace('  ', ' ')
    pointsdf.PLAYER = pointsdf.PLAYER.str.replace('Mohammed Shami', 'Mohammad Shami')

    m_query = 'select * from "maidens-19";'
    mdf = pd.read_sql_query(m_query,engine)
    mdf = mdf.drop(['index', 'POS', 'Inns', 'Ov', 'Mat', 'Runs', 'Wkts', 'Avg', 'Econ', 'SR', '4w', '5w'], axis=1)
    mdf.PLAYER = mdf.PLAYER.str.replace('  ', ' ')


    pdf = pd.read_csv("players.csv")
    pdf.PLAYER = pdf.PLAYER.str.replace('  ', ' ')


    mergedf = rundf.merge(wicketsdf, left_on='PLAYER', right_on='PLAYER', how='outer')
    mergedf = mergedf.merge(pointsdf, left_on='PLAYER', right_on='PLAYER', how='outer')
    mergedf = mergedf.merge(mdf, left_on='PLAYER', right_on='PLAYER', how='outer')
    mergedf = mergedf.merge(pdf, left_on='PLAYER', right_on='PLAYER', how='outer')

    df = mergedf.fillna(0)
    return df

def p2018():
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    runs_query = 'select * from "most-runs-18";'
    rundf = pd.read_sql_query(runs_query,engine)
    rundf = rundf.drop(['index', 'POS', 'NO', 'Mat', 'HS', 'Avg', 'BF'], axis=1)
    rundf.PLAYER = rundf.PLAYER.str.replace('  ', ' ')

    wickets_query = 'select * from "most-wickets-18";'
    wicketsdf = pd.read_sql_query(wickets_query,engine)
    wicketsdf = wicketsdf.drop(['index', 'POS', 'Ov', 'Mat', 'Runs', 'BBI', 'Avg', 'SR'], axis=1)
    wicketsdf.PLAYER = wicketsdf.PLAYER.str.replace('  ', ' ')

    points_query = 'select * from "points-18";'
    pointsdf = pd.read_sql_query(points_query,engine)
    pointsdf = pointsdf.drop(['index', 'POS', 'Pts', 'Wkts', 'Dots', '4s', '6s'], axis=1)
    pointsdf.PLAYER = pointsdf.PLAYER.str.replace('  ', ' ')
    pointsdf.PLAYER = pointsdf.PLAYER.str.replace('Mohammed Shami', 'Mohammad Shami')

    m_query = 'select * from "maidens-18";'
    mdf = pd.read_sql_query(m_query,engine)
    mdf = mdf.drop(['index', 'POS', 'Inns', 'Ov', 'Mat', 'Runs', 'Wkts', 'Avg', 'Econ', 'SR', '4w', '5w'], axis=1)
    mdf.PLAYER = mdf.PLAYER.str.replace('  ', ' ')


    pdf = pd.read_csv("players.csv")
    pdf.PLAYER = pdf.PLAYER.str.replace('  ', ' ')


    mergedf = rundf.merge(wicketsdf, left_on='PLAYER', right_on='PLAYER', how='outer')
    mergedf = mergedf.merge(pointsdf, left_on='PLAYER', right_on='PLAYER', how='outer')
    mergedf = mergedf.merge(mdf, left_on='PLAYER', right_on='PLAYER', how='outer')
    mergedf = mergedf.merge(pdf, left_on='PLAYER', right_on='PLAYER', how='outer')

    df = mergedf.fillna(0)
    return df



def m2():
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    df = p2019()
    df2 = p2018()



    df['TotalPTS'] = df['Runs']*1 + df['4s']*1 + df['6s']*2 + df['50']*8 + df['100']*16 + df['Wkts']*25 + df['4w']*8 + df['5w']*16 + df['Maid']*8 + df['Catches']*8 + df['Stumpings']*16 + df.apply(h, axis=1)*4 + df['Inns_y']*df.apply(f,axis=1) + df['Inns_x']*df.apply(g,axis=1)
    df['PTSPMAT'] = df['TotalPTS'] / df.apply(h, axis=1)
    df['PPP'] = df['PTSPMAT'] / df['Price']


    df2['TotalPTS18'] = df2['Runs']*1 + df2['4s']*1 + df['6s']*2 + df2['50']*8 + df2['100']*16 + df2['Wkts']*25 + df2['4w']*8 + df2['5w']*16 + df2['Maid']*8 + df2['Catches']*8 + df2['Stumpings']*16 + df2.apply(h, axis=1)*4 + df2['Inns_y']*df2.apply(f,axis=1) + df2['Inns_x']*df2.apply(g,axis=1)
    df2['PTSPMAT18'] = df2['TotalPTS18'] / df2.apply(h, axis=1)
    df2['PPP18'] = df2['PTSPMAT18'] / df2['Price']

    df = pd.merge(df,df2[['PLAYER','TotalPTS18', 'PTSPMAT18', 'PPP18']], on='PLAYER', how='left')



    df = df[df['Team'] != 0]
    df.Team = df.Team.str.strip()
    df = df.fillna(0)
    df = df[~df.isin([np.inf, -np.inf]).any(1)]


    df['CPTS'] = df.apply(i, axis=1)
    df['CPPM'] = df.apply(j, axis=1)
    df['CPPP'] = df.apply(k, axis=1)



    df = df.round(2)
    df = df.sort_values(by=['CPPM'], ascending=False)
    df = df.reset_index(drop=True)

    print df

    csk = df[df['Team']=='CSK']
    dc = df[df['Team']=='DC']

    kxi = df[df['Team']=='KXI']
    kkr = df[df['Team']=='KKR']

    mi = df[df['Team']=='MI']
    rr = df[df['Team']=='RR']

    rcb = df[df['Team']=='RCB']
    srh = df[df['Team']=='SRH']

    wk = df[df['Role']=='W']
    batsman = df[df['Role']=='B']
    allround = df[df['Role']=='A']
    bowl = df[df['Role']=='P']

    top = df[(df['CPPP']>5 ) & (df['Mat'] > 5)]
    top = top.sort_values(by=['CPPM'], ascending=False)
    sum = top.Price.sum()


    perf = df[df['TotalPTS']> 600]

#    print perf

#    print top
#    print sum

#    print wk
#    print batsman
#    print allround
#    print bowl

#    print csk
#    print dc
#    print kxi
#    print kkr
#    print mi
#    print rr
#    print rcb
#    print srh



#    print len(df)
#    df.to_sql('2019', engine)

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

def i(x):
    if x['TotalPTS'] == 0: return x['TotalPTS18']
    else: return x['TotalPTS']

def j(x):
    if x['PTSPMAT'] == 0: return x['PTSPMAT18']
    else: return x['PTSPMAT']

def k(x):
    if x['PPP'] == 0: return x['PPP18']
    else: return x['PPP']



def main():
    parser = argparse.ArgumentParser(description="Script creates tables for return")
    m2()


if __name__ == "__main__":
    main()
