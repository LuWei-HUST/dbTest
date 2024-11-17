import os
import table
import util
import re
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
        self.desc_pat = r"[ ]*desc[ ]+table[ ]+([1-9a-zA-Z_]+)[ ]*;"

    def parse(self, input_string):
        input_string = input_string.strip()

        match_flag = False

        r = re.match(self.desc_pat, input_string)
        if r:
            match_flag = True
            tbName = r.group(1)
            tmpT = util.getTableSchema(tbName)
            tmpT.showTable()

        r = re.match(self.drop_head_pat, input_string)
        if r:
            match_flag = True
            r_ = re.search(self.drop_pat, input_string)
            if r_:
                tbName = r_.group(1)
                util.dropTable(tbName)
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
                tmpT = util.getColumn(tbName, colNames)
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
                ret = util.createTable(tbName, colNames)
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
                
                util.insertValues(tbName, values)

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

                            if types[i] == "double":
                                flag = True
                                try:
                                    # tmp_data = int(df.iloc[:, i])
                                    tmp_data = list(df.iloc[:, i])
                                    tmp_data = [float(str(item)) for item in tmp_data]
                                    print(tmp_data)
                                    col_data.append(tmp_data)
                                except Exception as e:
                                    print("parse data failed")
                                    return

                            if types[i] == "int":
                                flag = True
                                try:
                                    # tmp_data = int(df.iloc[:, i])
                                    tmp_data = list(df.iloc[:, i])
                                    tmp_data = [int(str(item)) for item in tmp_data]
                                    col_data.append(tmp_data)
                                except Exception as e:
                                    print("parse data failed")
                                    return

                            if types[i] == "string":
                                flag = True
                                try:
                                    tmp_data = list(df.iloc[:, i])
                                    tmp_data = [str(item) for item in tmp_data]
                                    col_data.append(tmp_data)
                                except Exception as e:
                                    print("parse data failed")
                                    return
                            
                            if not flag:
                                print("error type")
                                return

                        for i in range(len_col):
                            colFilePath = os.path.join(tableDirPath, cols[i]+".wcol")
                            if types[i] == "int":
                                with open(colFilePath, 'ab') as fout:
                                    for j in col_data[i]:
                                        v = util.int_to_fixed_bytes(j)
                                        fout.write(v)
                            
                            if types[i] == "double":
                                with open(colFilePath, 'ab') as fout:
                                    for j in col_data[i]:
                                        v = util.double_to_fixed_bytes(j)
                                        fout.write(v)

                            if types[i] == "string":
                                with open(colFilePath, 'ab') as fout:
                                    for j in col_data[i]:
                                        v = util.string_to_fixed_bytes(j, 255)
                                        fout.write(v)

                        return len(col_data[0])

            else:
                print("syntax error: copy from filepath to tablename format 'sep';")

        if not match_flag:
            print("syntax error, match nothing")

if __name__ == "__main__":
    pass