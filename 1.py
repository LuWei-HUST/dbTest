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

def createTable(tbName):
    tmpT = table.Table(tbName)
    tablePath = os.path.join(tablePathBase, tbName+".wtbl")

    if os.path.exists(tablePath):
        print("table {} already exixts".format(tbName))
        return False
    else:
        f = open(tablePath, "w")
        f.close()
        
        i = 1
        while True:
            # input column
            col = input("col{}: ".format(i))
            if col == r"\q":
                break

            args = col.strip().split(" ")
            if len(args) != 2:
                print("syntax error")
            else:
                colName = args[0]
                colType = args[1]
                colPath = os.path.join(tablePathBase, colName+".wcol")
                with open(tablePath, "a") as f:
                    f.write(colName+","+colType+"\n")
                f = open(colPath, "w")
                f.close()
                
                tmpT.addColumn(colName)

            i += 1

        tDict[tbName] = copy.deepcopy(tmpT)
        tmpT = None

        return True

def showTable(tbName):
    tablePath = os.path.join(tablePathBase, tbName+".wtbl")
    # undone
    if os.path.exists(tablePath):
        if tbName in tDict.keys():
            tDict[tbName].showTable()
        else:
            tmpT = table.Table(tbName)
            with open(tablePath, 'r') as f:
                lines = f.readlines()
                cols = [i.strip().split(",")[0] for i in lines]
                ind = 0
                for c in cols:
                    tmpT.addColumn(c)
                    colFilePath = os.path.join(tablePathBase, c+".wcol")
                    if os.path.exists(colFilePath):
                        with open(colFilePath, 'r') as f:
                            lines = f.readlines()
                            colDatas = [i.strip() for i in lines]
                            tmpT.addColumnData(ind, colDatas)
                    else:
                        print("get column data error")
                    ind += 1
            
            tDict[tbName] = copy.deepcopy(tmpT)
            tDict[tbName].showTable()
    else:
        print("table {} not exists".format(tbName))

def parseCsv(csvPath, sep):
    pass

def dropTable(tbName):
    tablePath = os.path.join(tablePathBase, tbName+".wtbl")
    if os.path.exists(tablePath):
        with open(tablePath, "r") as f:
            lines = f.readlines()
            for l in lines:
                c = l.strip().split(",")[0]
                colFilePath = os.path.join(tablePathBase, c+".wcol")
                if os.path.exists(colFilePath):
                    os.remove(colFilePath)

        os.remove(tablePath)
        print("TABLE {} droped".format(tbName))
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
    select_pat = r"[ ]*select[ ]+([1-9a-zA-Z,_ \*]+)[ ]+from[ ]+([1-9a-zA-Z_\*]+)[ ]*;"

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
            tmpT.showTable()
        
        if ch == "create table":
            tbName = input("enter table name: ")
            r = createTable(tbName)
            if r:
                print("CREATE TABLE {}".format(tbName))
            else:
                print("CREATE DATABASE FAILED")

        if ch == "show table":
            tbName = input("enter table name: ")
            showTable(tbName)

        if ch.startswith("copy from"):
            args = ch.split(" ")

            if len(args) != 4:
                print("syntax error")
            else:
                csvPath = args[-2]
                sep = args[-1]
                parseCsv(csvPath, sep)

        if ch == "drop table":
            tbName = input("enter table name: ")
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


        