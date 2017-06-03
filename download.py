#!/usr/bin/env python3
import pandas as pd
import requests
from pyquery import PyQuery as pQ
import sys


def strip(df):
    for i, row in df.iterrows():
        statement = row.statement
        if 'select' not in statement.lower() or row.error != 0:
            df.drop(i, inplace=True)


def parse_tr(tr):
    tds = []
    for td in tr.children().items():
        tds.append(td.text())
    return tds


def download_one_table(name):
    payload = {
        'cmd': "select" + " * from %s" % name,
        'syntax': '',
        'format': 'html'
    }
    r = requests.post("http://cas.sdss.org/dr7/en/tools/search/x_sql.asp", data=payload)
    d = pQ(r.text)
    lines = d("body > table > tr")
    g = lines.items()
    columns = parse_tr(next(g))
    lines = []
    for tr in g:
        lines.append(parse_tr(tr))
    df = pd.DataFrame(columns=columns, data=lines)
    df.to_pickle("./data/" + name + '.pkl')


def main():
    toy = True
    if len(sys.argv) != 1:
        toy = False
    d = pQ(url="http://cas.sdss.org/dr7/en/help/browser/shortdescr.asp?n=Tables&t=U")
    a_name_s = d("table tr td.v a")
    names = []
    for one in a_name_s.items():
        names.append(one.html())
    if toy:
        names = names[:5]

    for n in names:
        print(n)
        download_one_table(n)


if __name__ == '__main__':
    main()
