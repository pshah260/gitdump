from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
from sqlalchemy import create_engine

r = requests.get("https://www.slickcharts.com/sp500")
soup = bs(r.content, "lxml")
table = soup.find_all("table")[0]
df = pd.read_html(str(table))[0]
df = df[["Company", "Symbol", "Weight"]]
engine = create_engine('postgresql://ai:ai@localhost:5432/stock')
df.to_sql('sp500', engine)
