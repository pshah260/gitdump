
from sqlalchemy import create_engine
import pandas as pd
import argparse
import psycopg2 as pg


def avg():
    engine = create_engine('postgresql://ai:ai@localhost:5432/stock')
    query = "select * from \"yearly\";"
    df = pd.read_sql_query(query,engine)

    query2 = "select * from \"CAGRSP500\";"
    df2 = pd.read_sql_query(query2,engine)


    df = df.drop(['index'], axis=1)
    df2 = df2.drop(['index'], axis=1)

    dfr = pd.DataFrame()
    dfr['Symbol'] = df['Symbol']
    dfr['Sum'] = df.sum(axis=1)
    dfr['Years'] = 20 - df[df.columns].eq(0).sum(axis=1)
    dfr['AVG'] = dfr['Sum'].div(dfr['Years'].values,axis=0)

    dfr['DIFF'] = dfr['AVG'] - df['1 Year Return']
    dfr['CAGR'] = df2['CAGR']

    dfr['Potential 1 year return'] = dfr['AVG'] + dfr['DIFF']

#    dfr = dfr[:200]
    dfr = dfr.sort_values(by=["Potential 1 year return"], ascending=False)
    dfr = dfr[dfr['CAGR']>24]
#    dfr = dfr[(dfr > 0).all(1)]
    dfr = dfr.round(2)
    dfr = dfr.drop(['Sum'], axis=1)
    print dfr
    dfr.to_sql('average', engine)

def db_table_delete():
    conn_string = "host='localhost' dbname='stock' user='ai' password='ai'"
    conn = pg.connect(conn_string)
    cursor = conn.cursor()
    conn.set_isolation_level(0)
    query = "drop table \"average\";"
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
    avg()

if __name__ == "__main__":
    main()
