import os
import re
import shutil
import table
import copy
from parser import SqlParser

tablePathBase = "/home/luwei/code/dbTest/storage"
curTable = "default"
tDict = {}

def insert():
    tablePath = os.path.join(tablePathBase, curTable+".wtbl")
    if os.path.exists(tablePath):
        cols = []
        with open(tablePath, 'r') as f:
            lines = f.readlines()
            for l in lines:
                c = l.strip().split(",")[0]
                cols.append(c)
        l = len(cols)
        i = 0
        chs = []
        while True:
            ch = input("col {} value: ".format(cols[i % l]))
            if ch == r"\q":
                break
            else:
                chs.append(ch)

            i += 1

        l_chs = i
        n = l_chs // l

        num = n * l
        chs_rerange = []
        for i in range(n):
            chs_rerange.append([])
        
        for i in range(num):
            chs_rerange[i % l].append(chs[i])

        for i in range(l):
            colFilePath = os.path.join(tablePathBase, cols[i]+".wcol")
            if os.path.exists(colFilePath):
                with open(colFilePath, 'a') as f:
                    l_col = len(chs_rerange[i])
                    for j in range(l_col):
                        f.write(chs_rerange[i][j] + "\n")
            else:
                #undone
                print("fatal error, col {} not exists".format(cols[i]))
                break

            
    else:
        print("TABLE {} NOT EXISTS".format(curTable))

if __name__ == "__main__":

    sqlparser = SqlParser()

    while True:
        ch = input("wsql#: ")

        if ch == "quit":
            exit()

        sqlparser.parse(ch)

        