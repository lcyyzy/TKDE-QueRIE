#!/usr/bin/env python3
import pandas as pd
import sys


def strip(df):
    for i, row in df.iterrows():
        statement = row.statement
        if 'select' not in statement.lower() or row.error != 0:
            df.drop(i, inplace=True)


def main():
    csv_path = "toyLogs.csv"
    if len(sys.argv) != 1:
        csv_path = sys.argv
    df = pd.read_csv(csv_path, names=[
        'yy',
        'mm',
        'dd',
        'hh',
        'mi',
        'ss',
        'seq',
        'theTime',
        'logID',
        'clientIP',
        'requester',
        'server',
        'dbName',
        'access',
        'elapsed',
        'busy',
        'rows',
        'statement',
        'error',
        'errorMsg',
        'isvisible',
    ])
    strip(df)
    print(df)


if __name__ == '__main__':
    main()
