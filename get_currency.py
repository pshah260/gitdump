from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from sqlalchemy import create_engine


df = pd.read_csv("currency.csv")
engine = create_engine('postgresql://ai:ai@localhost:5432/stock')
df.to_sql('currency', engine)
