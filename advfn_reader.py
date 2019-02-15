from bs4 import BeautifulSoup, SoupStrainer
import requests
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
from multiprocessing import Pool
import os

headers = {'User-Agent': 'Mozilla/5.0'}
only_table_tags = SoupStrainer('table')


def get_closest_quarter(target):
    #  candidate list
    candidates = [
        dt.date(target.year - 1, 12, 31),
        dt.date(target.year, 3, 31),
        dt.date(target.year, 6, 30),
        dt.date(target.year, 9, 30),
        dt.date(target.year, 12, 31),
    ]
    # return the minimum of the absolute distance.
    return min(candidates, key=lambda d: abs(target - d))


def parse_site(url):
    """Fetches the site with the quarterly results, returns them in a pandas data frame."""
    page = requests.get(url)
    # lxml parses slightly quicker than the html-parser
    soup = BeautifulSoup(page.text, 'lxml', parse_only=only_table_tags)
    if soup.find_all(lambda tag: tag.name == 'table' and tag.has_attr('cellpadding') and tag['cellpadding'] == '2') != []:
        table = soup.find_all(lambda tag: tag.name == 'table' and tag.has_attr(
            'cellpadding') and tag['cellpadding'] == '2')[1]
        table_rows = table.find_all('tr')
        l = []
        for tr in table_rows:
            td = tr.find_all('td')
            row = [j.text for j in td]
            l.append(row)
        dl = pd.DataFrame(l).T
        dl.columns = dl.iloc[0]
        dl.reindex(dl.index.drop(0))
        dl = dl.iloc[1:]
        dl = dl.set_index('quarter end date')
        return dl


def get_links(ticker, exchange, start_date, end_date):
    """Fetches the first page of the quarterlys for a given ticker and exchange and calculates the number of pages it needs to retrieve. 
    Returns an array with the links to every fifth page. Otherwise we would be querrying duplicate information."""
    url = 'https://de.advfn.com/borse/' + exchange + '/' + ticker + \
        '/finanzwerte?btn=istart_date&istart_date=1&mode=quarterly_reports'
    df = parse_site(url)
    start = dt.datetime.strptime(df.index[0], '%Y/%m')
    if end_date != dt.datetime.today().date():
        end_date = dt.datetime.strptime(end_date, '%Y/%m').date()
    num_of_pages = (relativedelta(get_closest_quarter(end_date), start.date(
    )).years * 4 + relativedelta(get_closest_quarter(end_date), start.date()).months / 4)
    # If start_date if given strip it and adjust the starting point.
    if start_date is not None:
        start_date = dt.datetime.strptime(start_date, '%Y/%m').date()
        i = (relativedelta(get_closest_quarter(start_date), start.date()).years *
             4 + relativedelta(get_closest_quarter(start_date), start.date()).months / 4)
    else:
        i = 1
    links = []
    while i < (num_of_pages):
        links.append('https://de.advfn.com/borse/' + exchange + '/' + ticker +
                     '/finanzwerte?btn=istart_date&istart_date=' + str((int(i))) + '&mode=quarterly_reports')
        i += 5
    return links


def get_quarts(ticker, exchange='NYSE', start_date=None, end_date=dt.datetime.today().date()):
    """Perfoms the multiprocess parsing of a given ticker and exchange. start_date and end_date are optional and adjust the range. Returns Pandas DataFrame all column names are lowercase."""
    quart_urls = get_links(ticker, exchange, start_date, end_date)
    # Creates available_threads - 2 processes if available_threads > 2.
    # Else create available_threads
    if len(os.sched_getaffinity(0)) > 2:
        cpus = len(os.sched_getaffinity(0))-2
    else:
        cpus = len(os.sched_getaffinity(0))
    with Pool(cpus) as p:
        records = p.map(parse_site, quart_urls)
        result = pd.concat(records, join='inner')
    return result
