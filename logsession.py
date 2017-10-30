import pandas as pd
import numpy as np
csv_path = "toy.csv"
#lc=pd.DataFrame(pd.read_csv('LoanStats3a.csv',header=1))
logs = pd.DataFrame(pd.read_csv(csv_path, names=[
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
    ]))
    #strip(logs)
#print(logs)
#logs.sort(columns='clientIP', ascending=True)
logs.sort_values(by=['clientIP', 'yy', 'mm', 'dd', 'hh', 'mi', 'ss'], ascending=True, inplace=True)
#logs.sort_values(by=["clientIP", "yy", "mm", "dd", "min", "ss"],ascending=True)
#print(logs)
flag_list = [0]
cnt = 0
#print(logs.loc[1, 'clientIP'])
#print(logs.__len__())
#print(logs.loc[0, 'clientIP'])

#logs = logs.set_index([range(logs.__len__()-1)])
#print(logs)
#sort_logs = pd.DataFrame(logs, index=range(logs.__len__()-1))
#print(sort_logs)
#print(logs.irow(0).loc['clientIP'])
index = list(logs.index)
#index = logs.ix[:, 0]
#print(index)
for i in range(logs.__len__()-2):
    cnt += 1
    if logs.ix[index[i]].loc['clientIP'] != logs.ix[index[i+1]].loc['clientIP']:
        #print(logs.loc[i, 'clientIP'], logs.loc[i+1, 'clientIP'])
        cnt = 0
        flag_list.append(i+1)
    if cnt == 500:
        flag_list.append(i+1)
        cnt = 0

print("session finished")
#print(flag_list)
#dfs = []
#print(logs.iloc[0:3, 0])
#for i in range(flag_list.__len__()-1):
    #print(flag_list[i])
    #print(flag_list[i+1])
    #dfs.append(logs.head(flag_list[i+1]-flag_list[i]))
    #dfs.append(logs.iloc[flag_list[i]:flag_list[i+1], :])
#print(dfs)