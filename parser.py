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
        self.insert_head_pat = "insert"
        self.insert_pat = r"[ ]*insert[ ]+into[ ]+([1-9a-zA-Z_]+)[ ]+values[ ]*\((.*?)\)[ ]*;"
        self.int_pat = r"(\d+)"
        self.string_pat = r"'(.*)'"

    def parse(self, input_string):
        input_string = input_string.strip().lower()

        match_flag = False
        r = re.match(self.drop_head_pat, input_string)
        if r:
            match_flag = True
            r_ = re.search(self.drop_pat, input_string)
            if r_:
                tbName = r_.group(1)
                dropTable(tbName)
            else:
                print("syntax error: drop table tablename;")

        r = re.match(self.select_head_pat, input_string)
        if r:
            match_flag = True
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
            match_flag = True
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

        r = re.match(self.insert_head_pat, input_string)
        if r:
            match_flag = True
            r_ = re.search(self.insert_pat, input_string)
            if r_:
                tbName = r_.group(1)
                # print(tbName)
                values = r_.group(2).strip().split(",")
                values = [i.strip() for i in values]

                values_parsed = []
                values_type = []
                flag = True
                for val in values:
                    v = re.search(self.string_pat, val)
                    if v:
                        values_parsed.append(v.group(1))
                        values_type.append("string")
                        continue

                    v = re.search(self.int_pat, val)
                    if v:
                        values_parsed.append(int(v.group(1)))
                        values_type.append("int")
                        continue
                    
                    flag = False
                    print("parse failed, syntax error or unsupported format")
                    break
                    

                # print(values)
                if flag:
                    insertValues(tbName, values_parsed, values_type)

        if not match_flag:
            print("syntax error, match nothing")

def insertValues(tbName, values, values_type):
    tablePathBase = os.path.join(util.getHomeDir(), "storage")
    tableDirPath = os.path.join(tablePathBase, tbName)

    if os.path.exists(tableDirPath):
        tableMetaPath = os.path.join(tableDirPath, tbName+".meta")
        if os.path.exists(tableMetaPath):
            with open(tableMetaPath, 'r') as f:
                lines = f.readlines()
                cols = [i.strip().split(" ")[0] for i in lines]
                if len(cols) != len(values):
                    print("column not match")
                    return
                
                types = [i.strip().split(" ")[1] for i in lines]
                len_types = len(types)
                for i in range(len_types):
                    if types[i].lower() != values_type[i].lower():
                        print("type not match")
                        return
                
                len_cols = len(cols)
                for i in range(len_cols):
                    colFilePath = os.path.join(tableDirPath, cols[i]+".wcol")
                    with open(colFilePath, 'a') as fout:
                        fout.write(str(values[i]))
                        fout.write("\n")

        else:
            print("table {} file damaged".format(tbName))
    else:
        print("table {} not exists".format(tbName))

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