from sqlalchemy import create_engine
import pandas as pd
import argparse
import psycopg2 as pg
from datetime import datetime
import numpy as np
from itertools import combinations
import sys

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)

def p2020():
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    runs_query = 'select * from "most-runs-20";'
    rundf = pd.read_sql_query(runs_query,engine)
    rundf = rundf.drop(['index', 'POS', 'NO', 'Mat', 'HS', 'Avg', 'BF'], axis=1)
    rundf.PLAYER = rundf.PLAYER.str.replace('  ', ' ')

    wickets_query = 'select * from "most-wickets-20";'
    wicketsdf = pd.read_sql_query(wickets_query,engine)
    wicketsdf = wicketsdf.drop(['index', 'POS', 'Ov', 'Mat', 'Runs', 'BBI', 'Avg', 'SR'], axis=1)
    wicketsdf.PLAYER = wicketsdf.PLAYER.str.replace('  ', ' ')

    points_query = 'select * from "points-20";'
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
    df = p2020()
    df2 = p2019()
    df3 = p2018()

    df['TotalPTS'] = df['Runs']*1 + df['4s']*1 + df['6s']*2 + df['50']*8 + df['100']*16 + df['Wkts']*25 + df['4w']*8 + df['5w']*16 + df['Maid']*8 +  df['Catches']*8 + df['Stumpings']*16 + df.apply(h, axis=1)*4 + df['Inns_y']*df.apply(f,axis=1) + df['Inns_x']*df.apply(g,axis=1)
    df['PTSPMAT'] = df['TotalPTS'] / df['Mat']
#    df['PTSPMAT'] = df['TotalPTS'] / df.apply(h, axis=1)
    df['PPP'] = df['PTSPMAT'] / df['Price']

    df2['TotalPTS19'] = df2['Runs']*1 + df2['4s']*1 + df2['6s']*2 + df2['50']*8 + df2['100']*16 + df2['Wkts']*25 + df2['4w']*8 + df2['5w']*16 + df2['Maid']*8 + df2['Catches']*8 + df2['Stumpings']*16 + df2.apply(h, axis=1)*4 + df2['Inns_y']*df2.apply(f,axis=1) + df2['Inns_x']*df2.apply(g,axis=1)
    df2['PTSPMAT19'] = df2['TotalPTS19'] / df2['Mat']
#    df2['PTSPMAT19'] = df2['TotalPTS19'] / df2.apply(h, axis=1)
    df2['PPP19'] = df2['PTSPMAT19'] / df2['Price']


    df3['TotalPTS18'] = df3['Runs']*1 + df3['4s']*1 + df3['6s']*2 + df3['50']*8 + df3['100']*16 + df3['Wkts']*25 + df3['4w']*8 + df3['5w']*16 + df3['Maid']*8 + df3['Catches']*8 + df3['Stumpings']*16 + df3.apply(h, axis=1)*4 + df3['Inns_y']*df3.apply(f,axis=1) + df3['Inns_x']*df3.apply(g,axis=1)
    df3['PTSPMAT18'] = df3['TotalPTS18'] / df3['Mat']
#    df3['PTSPMAT18'] = df3['TotalPTS18'] / df3.apply(h, axis=1)
    df3['PPP18'] = df3['PTSPMAT18'] / df3['Price']

    df = pd.merge(df,df2[['PLAYER','TotalPTS19', 'PTSPMAT19', 'PPP19']], on='PLAYER', how='left')
    df = pd.merge(df,df3[['PLAYER','TotalPTS18', 'PTSPMAT18', 'PPP18']], on='PLAYER', how='left')

    df = df[df['Team'] != 0]
    df.Team = df.Team.str.strip()
    df = df.replace([np.inf, -np.inf, np.nan], 0)

    df['CPTS'] = df.apply(i, axis=1)
    df['CPPM'] = df.apply(j, axis=1)
    df['CPPP'] = df.apply(k, axis=1)

    df = df.round(2)
    df = df.sort_values(by=['CPPM'], ascending=False)
    df = df.reset_index()

    df = df.drop(["index","Inns_x", "SR", "100", "50", "4s", "6s", "Maid", "Inns_y", "Econ", "4w", "5w", "Catches", "Stumpings"], axis=1)
    return df


def tprint(team):
    df = m2()
    tp = df[df['Team']== team ]
    print tp

def rprint(role):
    df = m2()
    rp = df[df['Role']== role]
    print rp

def pp(price):
    df = m2()
    ppr = df[df['Price']<=price]
    print ppr

def entire():
    df = m2()
    df = df.sort_values(by=['TotalPTS'], ascending=False)
    df = df.reset_index()
    print df

def t3(team, role, price):
    df = m2()
    t3 = df[(df['Team'] == team) & (df['Price'] <= float(price)) & (df['Role'] == role)]
    print t3

def rp(role, price):
    df = m2()
    rp = df[(df['Price'] <= float(price)) & (df['Role']== role)]
    print rp

def tp(team, price):
    df = m2()
    tp = df[(df['Team'] == team) & (df['Price'] <= float(price))]
    print tp

def tr(team, role):
    df = m2()
    tr = df[(df['Team'] == team) & (df['Role']== role)]
    print tr

def xtp(t1, t2):
    df = m2()
    df = df[(df['Team'] == t1) | (df['Team']== t2)]
    df = df.sort_values(by=['CPPM'], ascending=False)
    print df


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
    if x['TotalPTS'] != 0: return x['TotalPTS']
    elif x['TotalPTS'] == 0: return x['TotalPTS19']
    elif x['TotalPTS19'] == 0: return x['TotalPTS18']
    else: return x['TotalPTS']

def j(x):
    if x['PTSPMAT'] != 0: return x['PTSPMAT']
    elif x['PTSPMAT'] == 0: return x['PTSPMAT19']
    elif x['PTSPMAT19'] == 0: return x['PTSPMAT18']
    else: return x['PTSPMAT']

def k(x):
    if x['PPP'] != 0: return x['PPP']
    elif x['PPP'] == 0: return x['PPP19']
    elif x['PPP19'] == 0: return x['PPP18']
    else: return x['PPP']

def gamet(game):
    engine = create_engine('postgresql://ai:ai@localhost:5432/ipl')
    query = 'select * from "teamschedule";'
    df = pd.read_sql_query(query,engine)
    print df.iloc[[int(game-1), int(game), int(game + 1), int(game + 2) ]]
    return df.TeamA[game-1][0:3], df.TeamB[game-1][0:3]


def main():
    parser = argparse.ArgumentParser(description="Script creates tables for return")
    parser.add_argument('--team', help='Team - CSK, DC, KXI, KKR, MI, RR, RCB, SRH')
    parser.add_argument('--role', help='Role - B=Batsman, A=Allrounder, W=WicketKeeper, P=Bowler')
    parser.add_argument('--price', type=float, help='Under and equal to price needed')
    parser.add_argument('--t1', help='Team - CSK, DC, KXI, KKR, MI, RR, RCB, SRH')
    parser.add_argument('--t2', help='Team - CSK, DC, KXI, KKR, MI, RR, RCB, SRH')
    parser.add_argument('--game', type=float, help='Game number')

    team = parser.parse_args().team
    role = parser.parse_args().role
    price = parser.parse_args().price
    t1 = parser.parse_args().t1
    t2 = parser.parse_args().t2
    game = parser.parse_args().game


    if t1 and t2:
        xtp(t1,t2)
        sys.exit(1)
    elif game:
        result = gamet(game)
        xtp(result[0], result[1])
        sys.exit(1) 
    else:
        pass

    if team and price and role:
        t3(team, role, price)
        sys.exit(1)
    elif team and role:
        tr(team, role)
        sys.exit(1)
    elif role and price:
        rp(role, price)
        sys.exit(1)
    elif team and price:
        tp(team, price)
        sys.exit(1)
    elif team:
        tprint(team)
    elif role:
        rprint(role)
    elif price:
        pp(price)
    else:
        entire()



if __name__ == "__main__":
    main()
