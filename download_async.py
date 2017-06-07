#!/usr/bin/env python3
import pandas as pd
import asyncio
import aiohttp
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


async def download_one_table(session, name):
    payload = {
        'cmd': "select" + " * from %s" % name,
        'syntax': '',
        'format': 'html'
    }
    async with session.post("http://cas.sdss.org/dr7/en/tools/search/x_sql.asp", data=payload) as response:
        d = pQ(await response.text())
        lines = d("body > table > tr")
        g = lines.items()
        columns = parse_tr(next(g))
        lines = []
        for tr in g:
            lines.append(parse_tr(tr))
        df = pd.DataFrame(columns=columns, data=lines)
        df.to_pickle("./data/" + name + '.pkl')
        return name


def main():
    toy = True
    if len(sys.argv) != 1:
        toy = False
    d = pQ(url="http://cas.sdss.org/dr7/en/help/browser/shortdescr.asp?n=Tables&t=U")
    a_name_s = d("table tr td.v a")
    names = []
    for one in a_name_s.items():
        names.append(one.html())
    print(names)
    if toy:
        names = names[:5]

    loop = asyncio.get_event_loop()
    async def start_fetch():
        async with aiohttp.ClientSession(loop=loop) as session:
            fs = []
            for n in names:
                fs.append(download_one_table(session, n))
            for c in asyncio.as_completed(fs, loop=loop):
                name = await c
                print(name, "completed")
    loop.run_until_complete(start_fetch())


if __name__ == '__main__':
    main()
