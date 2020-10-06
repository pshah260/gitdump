from sqlalchemy import create_engine
import pandas as pd
import argparse
import psycopg2 as pg
from datetime import datetime

def cagr():
    stocks = get_meta()
    engine = create_engine('postgresql://ai:ai@localhost:5432/weekly')
    dfr = pd.DataFrame(columns=['Symbol', 'CAGR', 'Years'])
    for code in stocks['NASDAQ Symbol']:
        test_query = "select * from \"" + code + "\";"
        try:
            df = pd.read_sql_query(test_query,engine)
        except:
            continue
        c = calc(df)[0]
	d = calc(df)[1]
        dfr = dfr.append({'Symbol': code, 'CAGR': c, 'Years': d } , ignore_index=True)
    dfr = dfr.sort_values(by=["CAGR"], ascending=False)
    engine2 = create_engine('postgresql://ai:ai@localhost:5432/stock')
    dfr.to_sql('CAGR', engine2)

def get_meta():
    engine = create_engine('postgresql://ai:ai@localhost:5432/stock')
    query = "select \"NASDAQ Symbol\" from \"nasdaq\";"
    df = pd.read_sql_query(query,engine)
    return df

def db_table_delete():
    conn_string = "host='localhost' dbname='stock' user='ai' password='ai'"
    conn = pg.connect(conn_string)
    cursor = conn.cursor()
    conn.set_isolation_level(0)
    query = "drop table \"CAGR\";"
    try:
        cursor.execute(query)
    except:
        print "No table to be deleted"

def calc(df):
    try: 
        a = float(df.adj_close[df.index[-1]]) / float(df.adj_close[0])
        b = datetime.strptime(df["index"][df.index[-1]], '%Y-%m-%d').year - datetime.strptime(df["index"][df.index[0]], '%Y-%m-%d').year
	if b < 2:
	    output = 0
	else:
            output  = round((a**(1/float(b)) - 1) * 100, 2)
    except:
        output = 0
	b = 0
    return output, b

def main():
    parser = argparse.ArgumentParser(description="Script creates tables for return")
    parser.add_argument('delete', help='If all tables needed to be deleted')
    delete = parser.parse_args().delete
    if delete:
        db_table_delete()
    else:
        print "No table to be deleted"
    cagr()

if __name__ == "__main__":
    main()
