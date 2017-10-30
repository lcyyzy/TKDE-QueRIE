import os
import csv

remove_from = 0
remove_to = 1

j=0
for fpathe, dirs, fs in os.walk('./data/'):
    for f in fs:
        if (j != 0):

            with open(os.path.join(fpathe, f), "rb") as fp_in, open(os.path.join('./pro/', f), "wb") as fp_out:
                print repr(open("dbviewcols.csv").readline())
                reader = csv.reader(fp_in, delimiter=",")
                writer = csv.writer(fp_out, delimiter=",")
                i = 0
                for row in reader:
                    if (i != 0):
                        del row[remove_from:remove_to]
                        writer.writerow(row)
                    i = i + 1
        j = j + 1