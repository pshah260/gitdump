import shutil
import urllib2
from contextlib import closing

with closing(urllib2.urlopen('ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqtraded.txt')) as r:
    with open('nasdaqtraded.txt', 'wb') as f:
        shutil.copyfileobj(r, f)


