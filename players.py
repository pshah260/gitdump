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
    df = pd.read_csv("players.csv")
    df.to_sql('players', engine)


def main():
    parser = argparse.ArgumentParser(description="Script creates tables for return")
    batpoints()


if __name__ == "__main__":
    main()
