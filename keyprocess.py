import os
import csv

remove_from = 0
remove_to = 1

with open("./data/photoobjall.csv", "rb") as fp_in, open("./pro/field.txt", "wb") as fp_out:
    # print repr(open("dbviewcols.csv").readline())
    reader = csv.reader(fp_in, delimiter=",")
    writer = csv.writer(fp_out, delimiter=",")
    for row in reader:
        for i, j in enumerate(row):
            if (i != 0):
                print(j + ' FLOAT(8),')
        break;
