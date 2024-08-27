import os
import table
import util
import re
import shutil

class SqlParser:

    def __init__(self) -> None:
        self.drop_head_pat = "drop"
        self.drop_pat = r"[ ]*drop[ ]+table[ ]+([1-9a-zA-Z_]+)[ ]*;"
        self.select_head_pat = "select"
        self.select_pat = r"[ ]*select[ ]+([1-9a-zA-Z,_ \*]+)[ ]+from[ ]+([1-9a-zA-Z_]+)[ ]*;"
        self.create_head_pat = "create"
        self.create_pat = r"[ ]*create[ ]+table[ ]+([1-9a-zA-Z_]+)\(([1-9a-zA-Z, _]+)\)[ ]*;"

    def parse(self, input_string):
        input_string = input_string.strip().lower()

        r = re.match(self.drop_head_pat, input_string)
        if r:
            r_ = re.search(self.drop_pat, input_string)
            if r_:
                tbName = r_.group(1)
                dropTable(tbName)
            else:
                print("syntax error: drop table tablename;")

        r = re.match(self.select_head_pat, input_string)
        if r:
            r_ = re.search(self.select_pat, input_string)
            if r_:
                colNamesText = r_.group(1)
                colNames = colNamesText.strip().split(",")
                colNames = [i.strip() for i in colNames]
                # print(colNames)
                tbName = r_.group(2)
                # print(tbName)
                tmpT = getColumn(tbName, colNames)
                if tmpT:
                    tmpT.showTable()
            else:
                print("syntax error: select colnames from tablename;")

        r = re.match(self.create_head_pat, input_string)
        if r:
            r_ = re.search(self.create_pat, input_string)
            if r_:
                tbName = r_.group(1)
                colNames = r_.group(2).strip().split(",")
                colNames = [i.strip() for i in colNames]
                tmpCols = colNames[:]
                colNames = []
                for c in tmpCols:
                    tmp_ = c.split(" ")
                    tmp_ = [i.strip() for i in tmp_]
                    tmp_ = list(filter(lambda x: x and x.strip(), tmp_))
                    tmp_ = " ".join(tmp_)
                    colNames.append(tmp_)
                ret = createTable(tbName, colNames)
                if ret:
                    print("CREATE TABLE {}".format(tbName))
                else:
                    print("CREATE DATABASE FAILED")
            else:
                print("syntax error: create table tablename(col1 type1, col2 type2, ...);")

def createTable(tbName, colNames):
    tablePathBase = os.path.join(util.getHomeDir(), "storage")
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
    
def dropTable(tbName):
    tablePathBase = os.path.join(util.getHomeDir(), "storage")
    tableDirPath = os.path.join(tablePathBase, tbName)
    if os.path.exists(tableDirPath):
        shutil.rmtree(tableDirPath)

        print("TABLE {} DROPED".format(tbName))
    else:
        print("TABLE {} NOT EXISTS".format(tbName))

def getColumn(tbName, colNames):
    tablePathBase = os.path.join(util.getHomeDir(), "storage")
    tableDirPath = os.path.join(tablePathBase, tbName)
    tableMetaPath = os.path.join(tableDirPath, tbName+".meta")
    # undone
    if os.path.exists(tableDirPath):
        if os.path.exists(tableMetaPath):
            tmpT = table.Table(tbName)
            with open(tableMetaPath, 'r') as f:
                lines = f.readlines()
                cols = [i.strip().split(" ")[0] for i in lines]

                for c in colNames:
                    if c not in cols and c != "*":
                        print("column {} not exists in table {}".format(c, tbName))
                        return

                ind = 0
                for c in colNames:
                    if c in cols:
                        colFilePath = os.path.join(tableDirPath, c+".wcol")
                        if os.path.exists(colFilePath):
                            tmpT.addColumn(c)
                            with open(colFilePath, 'r') as f:
                                lines = f.readlines()
                                colDatas = [i.strip() for i in lines]
                                tmpT.addColumnData(ind, colDatas)
                            ind += 1
                        else:
                            print("get column data error")
                    elif c == "*":
                        for c_ in cols:
                            colFilePath = os.path.join(tableDirPath, c_+".wcol")
                            if os.path.exists(colFilePath):
                                tmpT.addColumn(c_)
                                with open(colFilePath, 'r') as f:
                                    lines = f.readlines()
                                    colDatas = [i.strip() for i in lines]
                                    tmpT.addColumnData(ind, colDatas)
                                ind += 1
                            else:
                                print("get column data error")
            return tmpT
        else:
            print("table {} file damaged".format(tbName))
    else:
        print("table {} not exists".format(tbName))

if __name__ == "__main__":
    pass