import os
import table
import util
import re
import shutil
import pandas as pd

STRING_PAT = r"^'(.*)'$"
INT_PAT = r"^(\d+)$"
DOUBLE_PAT_1 = r"^(\d+\.\d+)$"
DOUBLE_PAT_2 = r"^(\d+)$"

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
        self.copy_head_pat = "copy"
        self.copy_pat = r"[ ]*copy[ ]+from[ ]+([/a-zA-Z0-9_\.]+)[ ]+to[ ]+([1-9a-zA-Z_]+)[ ]+format[ ]+'(.)'[ ]*;"

    def parse(self, input_string):
        input_string = input_string.strip()

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
                
                insertValues(tbName, values)

            else:
                print("syntax error: insert into tablename values(val1, val2, ...);")

        r = re.match(self.copy_head_pat, input_string)
        if r:
            match_flag = True
            r_ = re.search(self.copy_pat, input_string)
            if r_:
                filePath = r_.group(1)
                tbName = r_.group(2)
                sep = r_.group(3)
                tableDirPath = os.path.join(util.getHomeDir(), "storage", tbName)
                tableMatePath = os.path.join(tableDirPath, tbName+".meta")

                # print(filePath)
                if not os.path.exists(filePath):
                    print("No such file or directory")
                    return
                elif not os.path.exists(tableMatePath):
                    print("get {}.meta failed".format(tbName))
                    return
                else:
                    with open(tableMatePath, "r") as fin:
                        lines = fin.readlines()
                        cols = [i.strip().split(" ")[0] for i in lines]
                        types = [i.strip().split(" ")[1] for i in lines]

                        df = pd.read_csv(filePath, sep=sep)
                        len_col = df.shape[1]

                        if len_col != len(cols):
                            print("column not match")
                            return

                        col_data = []
                        
                        for i in range(len_col):
                            flag = False
                            if types[i] == "int":
                                flag = True
                                try:
                                    # tmp_data = int(df.iloc[:, i])
                                    tmp_data = list(df.iloc[:, i])
                                    tmp_data = [str(item) for item in tmp_data]
                                    col_data.append(tmp_data)
                                except Exception as e:
                                    print("parse data failed")
                                    return

                            if types[i] == "string":
                                flag = True
                                try:
                                    tmp_data = list(df.iloc[:, i])
                                    col_data.append(tmp_data)
                                except Exception as e:
                                    print("parse data failed")
                                    return
                            
                            if not flag:
                                print("error type")
                                return

                        for i in range(len_col):
                            colFilePath = os.path.join(tableDirPath, cols[i]+".wcol")
                            with open(colFilePath, 'a') as fout:
                                fout.writelines([item+"\n" for item in col_data[i]])

                        return len(col_data[0])

            else:
                print("syntax error: copy from filepath to tablename format 'sep';")

        if not match_flag:
            print("syntax error, match nothing")

def insertValues(tbName, values):
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

                values_parsed = []

                for i in range(len_types):
                    if types[i].lower() == "string":
                        v = re.search(STRING_PAT, values[i])
                        if v:
                            values_parsed.append(v.group(1))
                        else:
                            print("value '{}' parse failed".format(values[i]))
                            return
                    
                    if types[i].lower() == "int":
                        v = re.search(INT_PAT, values[i])
                        if v:
                            values_parsed.append(int(v.group(1)))
                        else:
                            print("value '{}' parse failed".format(values[i]))
                            return

                    if types[i].lower() == "double":
                        v = re.search(DOUBLE_PAT_1, values[i])
                        if v:
                            values_parsed.append(float(v.group(1)))
                        else:
                            v = re.search(DOUBLE_PAT_2, values[i])
                            if v:
                                values_parsed.append(float(v.group(1)))
                            else: 
                                print("value '{}' parse failed".format(values[i]))
                                return

                values = values_parsed
                
                len_cols = len(cols)
                for i in range(len_cols):
                    colFilePath = os.path.join(tableDirPath, cols[i]+".wcol")
                    with open(colFilePath, 'ab') as fout:
                        if types[i].lower() == "string":
                            d_ = util.string_to_fixed_bytes(values[i], 255)
                            fout.write(d_)

                        if types[i].lower() == "int":
                            d_ = util.int_to_fixed_bytes(values[i])
                            fout.write(d_)

                        if types[i].lower() == "double":
                            d_ = util.double_to_fixed_bytes(values[i])
                            fout.write(d_)


        else:
            print("table {} file damaged".format(tbName))
    else:
        print("table {} not exists".format(tbName))

def createTable(tbName, colNames):
    tablePathBase = os.path.join(util.getHomeDir(), "storage")
    tableDirPath = os.path.join(tablePathBase, tbName)

    if not os.path.exists(tablePathBase):
        os.mkdir(tablePathBase)

    for c in colNames:
        colType = c.split(" ")[1]
        if colType.lower() not in ["int", "string", "double"]:
            print("Only support INT, STRING and DOUBLE .")
            return False

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
                types = [i.strip().split(" ")[1] for i in lines]

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
                            with open(colFilePath, 'rb') as f:
                                index = cols.index(c)
                                tp = types[index]

                                colDatas = []
                                if tp.lower() == "string":
                                    len = int(f.seek(0, 2) / 255)
                                    f.seek(0)

                                    for j in range(len):
                                        s = f.read(255)
                                        len_head = int.from_bytes(s[:2], "big")
                                        s = s[2:len_head+2]
                                        d = s.decode(encoding='UTF-8',errors='strict')
                                        colDatas.append(d)

                                if tp.lower() == "int":
                                    len = int(f.seek(0, 2) / 4)
                                    f.seek(0)

                                    for j in range(len):
                                        s = f.read(4)
                                        d = util.int_from_fixed_bytes(s)
                                        colDatas.append(d)

                                if tp.lower() == "double":
                                    len = int(f.seek(0, 2) / 8)
                                    f.seek(0)

                                    for j in range(len):
                                        s = f.read(8)
                                        d = util.double_from_fixed_bytes(s)
                                        colDatas.append(d)

                                tmpT.addColumnData(ind, colDatas)
                            ind += 1
                        else:
                            print("get column data error")
                    elif c == "*":
                        # print(cols)
                        for c_ in cols:
                            colFilePath = os.path.join(tableDirPath, c_+".wcol")
                            if os.path.exists(colFilePath):
                                tmpT.addColumn(c_)
                                with open(colFilePath, 'rb') as f:
                                    index = cols.index(c_)
                                    tp = types[index]

                                    colDatas = []
                                    if tp.lower() == "string":
                                        len = int(f.seek(0, 2) / 255)
                                        f.seek(0)

                                        for j in range(len):
                                            s = f.read(255)
                                            len_head = int.from_bytes(s[:2], "big")
                                            s = s[2:len_head+2]
                                            d = s.decode(encoding='UTF-8',errors='strict')
                                            colDatas.append(d)

                                    if tp.lower() == "int":
                                        len = int(f.seek(0, 2) / 4)
                                        f.seek(0)

                                        for j in range(len):
                                            s = f.read(4)
                                            d = util.int_from_fixed_bytes(s)
                                            colDatas.append(d)

                                    if tp.lower() == "double":
                                        len = int(f.seek(0, 2) / 8)
                                        f.seek(0)

                                        for j in range(len):
                                            s = f.read(8)
                                            d = util.double_from_fixed_bytes(s)
                                            colDatas.append(d)
                                    # print(colDatas)
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