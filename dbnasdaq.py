from sqlalchemy import create_engine
import pandas as pd
import argparse
import psycopg2 as pg

def dbnasdaq():
    file = open('nasdaqtraded.txt', 'r')
    lines = file.readlines()
    lines = lines[:-1]
    lines = map(str.strip, lines)
    df = pd.DataFrame(lines)
    df2 = df[0].str.split("|", expand=True)
    df2.columns = df2.iloc[0]
    df3 = df2[1:]
    engine = create_engine('postgresql://ai:ai@localhost:5432/stock')
    df3.to_sql('nasdaq', engine)


def db_table_delete():
    conn_string = "host='localhost' dbname='stock' user='ai' password='ai'"
    conn = pg.connect(conn_string)
    cursor = conn.cursor()
    conn.set_isolation_level(0)
    query = "drop table \"nasdaq\";"
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
    dbnasdaq()

if __name__ == "__main__":
    main()
