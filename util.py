import os
import table
import struct
import re
import shutil

STRING_PAT = r"^'(.*)'$"
INT_PAT = r"^(\d+)$"
DOUBLE_PAT_1 = r"^(\d+\.\d+)$"
DOUBLE_PAT_2 = r"^(\d+)$"

TABLES = {}

def createTable(tbName, colNames):
    tablePathBase = os.path.join(getHomeDir(), "storage")
    tableDirPath = os.path.join(tablePathBase, tbName)

    if not os.path.exists(tablePathBase):
        os.mkdir(tablePathBase)

    for c in colNames:
        colType = c.split(" ")[1]
        if colType.lower() not in ["int", "string", "double"]:
            print("Only support INT, STRING and DOUBLE .")
            return False

    if tbName in TABLES.keys():
        print("table {} already exixts".format(tbName))
        return False
    else:
        tmp = table.Table(tbName)
        
        for c in colNames:
            colName = c.split(" ")[0]
            tmp.addColumn(colName)

        TABLES[tbName] = tmp

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

def insertValues(tbName, values):
    tablePathBase = os.path.join(getHomeDir(), "storage")
    tableDirPath = os.path.join(tablePathBase, tbName)

    if tbName in TABLES.keys():
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

                    for i in range(TABLES[tbName].colNum):
                        TABLES[tbName].addColumnData(i, [values[i]])
                        
            else:
                print("table {} file damaged".format(tbName))
                return
        else:
            print("table {} not exists".format(tbName))
            return
            

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
                            d_ = string_to_fixed_bytes(values[i], 255)
                            fout.write(d_)

                        if types[i].lower() == "int":
                            d_ = int_to_fixed_bytes(values[i])
                            fout.write(d_)

                        if types[i].lower() == "double":
                            d_ = double_to_fixed_bytes(values[i])
                            fout.write(d_)


        else:
            print("table {} file damaged".format(tbName))
    else:
        print("table {} not exists".format(tbName))

def dropTable(tbName):
    tablePathBase = os.path.join(getHomeDir(), "storage")
    tableDirPath = os.path.join(tablePathBase, tbName)
    if os.path.exists(tableDirPath):
        shutil.rmtree(tableDirPath)

        print("TABLE {} DROPED".format(tbName))
    else:
        print("TABLE {} NOT EXISTS".format(tbName))

def getColumn(tbName, colNames):
    tablePathBase = os.path.join(getHomeDir(), "storage")
    tableDirPath = os.path.join(tablePathBase, tbName)
    tableMetaPath = os.path.join(tableDirPath, tbName+".meta")
    # undone

    if tbName in TABLES.keys():
        for c in colNames:
            if c not in TABLES[tbName].colNames and c != "*":
                print("column {} not exists in table {}".format(c, tbName))
                return
        
        tmpT = table.Table(tbName)
        inx = 0
        for c in colNames:
            if c == "*":
                for c_ in TABLES[tbName].colNames:
                    tmpT.addColumn(c_)
                    i = TABLES[tbName].colNames.index(c_)
                    tmpT.addColumnData(inx, TABLES[tbName].t[i])
                    inx += 1
            else:
                i = TABLES[tbName].colNames.index(c)
                tmpT.addColumn(c)
                tmpT.addColumnData(TABLES[tbName].t[i])
                inx += 1

        return tmpT
                

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
                                        d = int_from_fixed_bytes(s)
                                        colDatas.append(d)

                                if tp.lower() == "double":
                                    len = int(f.seek(0, 2) / 8)
                                    f.seek(0)

                                    for j in range(len):
                                        s = f.read(8)
                                        d = double_from_fixed_bytes(s)
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
                                            d = int_from_fixed_bytes(s)
                                            colDatas.append(d)

                                    if tp.lower() == "double":
                                        len = int(f.seek(0, 2) / 8)
                                        f.seek(0)

                                        for j in range(len):
                                            s = f.read(8)
                                            d = double_from_fixed_bytes(s)
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

def getHomeDir():
    return os.path.dirname(os.path.abspath(__file__))

def getAllTables():
    tmpT = table.Table("AllTables")
    tableBasePath = os.path.join(getHomeDir(), "storage")
    tableNames = []
    for root, dirs, files in os.walk(tableBasePath):
        for name in dirs:
            tableNames.append(name)

    tmpT.addColumn("name")
    tmpT.addColumnData(0, tableNames)

    return tmpT

def string_to_fixed_bytes(s, length):
    length -= 2
    if len(s.encode('utf-8')) > length:
        raise ValueError("The string is longer than the specified length.")
    return struct.pack(">H", len(s.encode('utf-8'))) + s.encode('utf-8') + (' ' * (length - len(s.encode('utf-8')))).encode('utf-8')

def int_to_fixed_bytes(v):
    return struct.pack(">i", v)

def int_from_fixed_bytes(d):
    return struct.unpack(">i", d)[0]

def double_to_fixed_bytes(v):
    return struct.pack(">d", v)

def double_from_fixed_bytes(d):
    return struct.unpack(">d", d)[0]

if __name__ == "__main__":
    print(getHomeDir())