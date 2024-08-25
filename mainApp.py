import os
import re
import shutil
import table
import copy
import parser

tablePathBase = "/home/luwei/code/dbTest/storage"
curTable = "default"
tDict = {}

def createDB(dbName):
    pass

def createTable(tbName, colNames):
    tableDirPath = os.path.join(tablePathBase, tbName)

    if os.path.exists(tableDirPath):
        print("table {} already exixts".format(tbName))
        return False
    else:
        os.mkdir(tableDirPath)
        tableMetaPath = os.path.join(tableDirPath, tbName+".meta")
        with open(tableMetaPath, "w") as f:
            f.writelines([i+"\n" for i in colNames])

        for c in colNames:
            colName = c.split(" ")[0]
            colPath = os.path.join(tableDirPath, colName+".wcol")
            with open(colPath, "w") as f:
                pass

        return True

def parseCsv(csvPath, sep):
    pass

def dropTable(tbName):
    tableDirPath = os.path.join(tablePathBase, tbName)
    if os.path.exists(tableDirPath):
        shutil.rmtree(tableDirPath)

        print("TABLE {} DROPED".format(tbName))
    else:
        print("TABLE {} NOT EXISTS".format(tbName))

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
    select_pat = r"[ ]*select[ ]+([1-9a-zA-Z,_ \*]+)[ ]+from[ ]+([1-9a-zA-Z_]+)[ ]*;"
    create_pat = r"[ ]*create[ ]+table[ ]+([1-9a-zA-Z_]+)\(([1-9a-zA-Z, _]+)\)[ ]*;"
    drop_pat = r"[ ]*drop[ ]+table[ ]+([1-9a-zA-Z_]+)[ ]*;"

    while True:
        ch = input("wsql#: ")

        if ch == "quit":
            exit()

        # if ch == "create db":
        #     dbName = input("enter db name: ")
        #     r = createDB(dbName)
        #     if r:
        #         print("CREATE DATABASE {}".format(dbName))
        #     else:
        #         print("CREATE DATABASE FAILED")
        res = re.search(select_pat, ch)
        if res:
            colNamesText = res.group(1)
            colNames = colNamesText.strip().split(",")
            colNames = [i.strip() for i in colNames]
            # print(colNames)
            tbName = res.group(2)
            # print(tbName)
            tmpT = parser.getColumn(tbName, colNames)
            if tmpT:
                tmpT.showTable()
        
        res = re.search(create_pat, ch)
        if res:
            tbName = res.group(1)
            colNames = res.group(2).strip().split(",")
            colNames = [i.strip() for i in colNames]
            tmpCols = colNames[:]
            colNames = []
            for c in tmpCols:
                tmp_ = c.split(" ")
                tmp_ = [i.strip() for i in tmp_]
                tmp_ = list(filter(lambda x: x and x.strip(), tmp_))
                tmp_ = " ".join(tmp_)
                colNames.append(tmp_)
            r = createTable(tbName, colNames)
            if r:
                print("CREATE TABLE {}".format(tbName))
            else:
                print("CREATE DATABASE FAILED")

        if ch.startswith("copy from"):
            args = ch.split(" ")

            if len(args) != 4:
                print("syntax error")
            else:
                csvPath = args[-2]
                sep = args[-1]
                parseCsv(csvPath, sep)

        res = re.search(drop_pat, ch)
        if res:
            tbName = res.group(1)
            dropTable(tbName)

        if ch == "use table":
            tbName = input("enter table name: ")
            tablePath = os.path.join(tablePathBase, tbName+".wtbl")
            if os.path.exists(tablePath):
                curTable = tbName
            else:
                print("TABLE {} NOT EXISTS".format(tbName))

        if ch == "insert":
            insert()


        