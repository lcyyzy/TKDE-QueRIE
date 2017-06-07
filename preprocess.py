#!/usr/bin/env python3
import pandas as pd
import sys
import sqlite3


def strip(df):
    for i, row in df.iterrows():
        statement = row.statement.lower()
        if 'select' not in statement or row.error != 0:
            df.drop(i, inplace=True)
        if 'insert' in statement:
            df.drop(i, inplace=True)
        if 'delete' in statement:
            df.drop(i, inplace=True)
        if 'update' in statement:
            df.drop(i, inplace=True)


def main():
    csv_path = "toyLogs.csv"
    table_names = ['Algorithm', 'Ap7Mag', 'BestTarget2Sector', 'Chunk', 'DataConstants', 'DBColumns', 'DBObjects',
                   'DBViewCols', 'Dependency', 'DR3QuasarCatalog', 'DR5QuasarCatalog', 'ELRedShift', 'Field',
                   'FieldProfile', 'FieldQA', 'First', 'Frame', 'Glossary', 'HalfSpace', 'History', 'HoleObj',
                   'Inventory', 'LoadHistory', 'Mask', 'MaskedObject', 'Match', 'MatchHead', 'Neighbors', 'ObjMask',
                   'OrigField', 'OrigPhotoObjAll', 'PhotoObjAll', 'PhotoProfile', 'PhotoTag', 'Photoz', 'Photoz2',
                   'PlateX', 'ProfileDefs', 'ProperMotions', 'PsObjAll', 'QsoBest', 'QsoBunch', 'QsoCatalogAll',
                   'QsoConcordanceAll', 'QsoSpec', 'QsoTarget', 'QueryResults', 'RC3', 'RecentQueries', 'Region',
                   'Region2Box', 'RegionArcs', 'RegionPatch', 'Rmatrix', 'Rosat', 'RunQA', 'RunShift',
                   'SDSSConstants', 'Sector', 'Sector2Tile', 'Segment', 'SiteConstants', 'SiteDBs', 'SiteDiagnostics',
                   'SpecLineAll', 'SpecLineIndex', 'SpecObjAll', 'SpecPhotoAll', 'sppLines', 'sppParams', 'Stetson',
                   'StripeDefs', 'TableDesc', 'Target', 'TargetInfo', 'TargetParam', 'TargPhotoObjAll', 'TargPhotoTag',
                   'TargRunQA', 'TileAll', 'TiledTargetAll', 'TilingGeometry', 'TilingInfo', 'TilingNote', 'TilingRun',
                   'UberAstro', 'UberCal', 'USNO', 'Versions', 'XCRedshift', 'Zone']
    if len(sys.argv) != 1:
        csv_path = sys.argv
    else:
        table_names = table_names[:5]

    logs = pd.read_csv(csv_path, names=[
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
    strip(logs)
    preprocessed_matrix = pd.DataFrame(index=logs.index)
    n_logs = len(logs)
    for name in table_names:
        data = pd.read_pickle('./data/' + name + '.pkl')
        n_max = len(data) * n_logs
        for k, (i, row) in enumerate(data.iterrows()):
            conn = sqlite3.connect(':memory:')
            row.to_sql(name, conn)
            s = pd.Series()
            col_name = "%s_%d" % (name, i)
            for u, (j, l) in enumerate(logs.iterrows()):
                print('\b\b\b\b\b\b\b\b%06.2f%% ' % ((k * n_logs + j) / n_max * 100), end='', flush=True)
                sql = l['statement']
                try:
                    cur = conn.execute(sql)
                    n = len(cur.fetchall())
                except sqlite3.OperationalError:
                    n = 0
                if n == 0:
                    s.loc[j] = 0
                else:
                    s.loc[j] = 1
            if s.sum() == 0:
                continue
            preprocessed_matrix[col_name] = s
        print('\b\b\b\b\b\b\b\b100.00% ', end='', flush=True)
        print("%s finished" % name)
    preprocessed_matrix = preprocessed_matrix.div(preprocessed_matrix.sum(axis=1), axis=0)
    preprocessed_matrix.dropna(inplace=True)
    print(preprocessed_matrix)


if __name__ == '__main__':
    main()
