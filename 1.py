import os
import shutil

tablePathBase = "/home/luwei/code/dbTest/storage"
curTable = "default"

def createDB(dbName):
    pass

def createTable(tbName):
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
                pass

            i += 1

        return True

def showTable(tbName):
    tablePath = os.path.join(tablePathBase, tbName+".wtbl")

    if os.path.exists(tablePath):
        with open(tablePath, "r") as f:
            print(f.read())
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
        # undone
    else:
        print("TABLE {} NOT EXISTS".format(tbName))

if __name__ == "__main__":
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


        