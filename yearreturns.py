from sqlalchemy import create_engine
import pandas as pd
import argparse
import psycopg2 as pg


def get_returns():
    stocks = get_meta()
    engine = create_engine('postgresql://ai:ai@localhost:5432/sp500')
    dfr = pd.DataFrame(columns=['Symbol', '1 Year Return', '2 Year Return', '3 Year Return', '4 Year Return' , '5 Year Return', '6 Year Return', '7 Year Return', '8 Year Return', '9 Year Return', '10 Year Return', '11 Year Return', '12 Year Return', '13 Year Return', '14 Year Return', '15 Year Return', '16 Year Return', '17 Year Return', '18 Year Return', '19 Year Return', '20 Year Return'])
    for code in stocks['Symbol']:
        test_query = "select * from \"" + code + "\";"

        try:
            df = pd.read_sql_query(test_query,engine)
        except:
            continue
        rlist = years(df)
        try:
            for n in range(len(rlist), 20):
                rlist.append(0)
            dfr = dfr.append({'Symbol': code, '1 Year Return': rlist[0], '2 Year Return': rlist[1], '3 Year Return': rlist[2], '4 Year Return': rlist[3], '5 Year Return': rlist[4], '6 Year Return': rlist[5], '7 Year Return': rlist[6], '8 Year Return': rlist[7], '9 Year Return': rlist[8], '10 Year Return': rlist[9], '11 Year Return': rlist[10], '12 Year Return': rlist[11], '13 Year Return': rlist[12], '14 Year Return': rlist[13], '15 Year Return': rlist[14] , '16 Year Return': rlist[15], '17 Year Return': rlist[16], '18 Year Return': rlist[17] , '19 Year Return': rlist[18], '20 Year Return': rlist[19] }, ignore_index=True)
        except:
            pass
    engine2 = create_engine('postgresql://ai:ai@localhost:5432/stock')
    dfr.to_sql('yearly', engine2)

def get_meta():
    engine = create_engine('postgresql://ai:ai@localhost:5432/stock')
    query = "select \"Symbol\" from \"sp500\";"
    df = pd.read_sql_query(query,engine)
    return df

def db_table_delete():
    conn_string = "host='localhost' dbname='stock' user='ai' password='ai'"
    conn = pg.connect(conn_string)
    cursor = conn.cursor()
    conn.set_isolation_level(0)
    query = "drop table \"yearly\";"
    try:
        cursor.execute(query)
    except:
        print "No table to be deleted"

def years(df):
    rlist = []
    for i in range(-1, (len(df)-52)*(-1), -52):
        try:
            rlist.append(round((float(df.iloc[df.index[i]]["adj_close"]) - float(df.iloc[df.index[int(i-52)]]["adj_close"]))/float(df.iloc[df.index[i]]["adj_close"])*100, 2))
        except:
            rlist = []
    return rlist

def main():
    parser = argparse.ArgumentParser(description="Script creates tables for return")
    parser.add_argument('delete', help='If all tables needed to be deleted')
    delete = parser.parse_args().delete
    if delete:
        db_table_delete()
    else:
        print "No table to be deleted"
    get_returns()

if __name__ == "__main__":
    main()
