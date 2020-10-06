
from sqlalchemy import create_engine
import pandas as pd
import argparse
import psycopg2 as pg


def main():
    engine = create_engine('postgresql://ai:ai@localhost:5432/stock')

    query = "select * from \"CAGR\";"
    df = pd.read_sql_query(query,engine)
    df = df.drop(['index'], axis=1)
    df = df.sort_values(by=["CAGR"], ascending=False)
    df = df[df['CAGR']>35]
#    df = df[df['CAGR']<75]
    print df


if __name__ == "__main__":
    main()
