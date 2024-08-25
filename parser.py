import os
import table
import util

def getColumn(tbName, colNames):
    tablePathBase = os.path.join(util.getHomeDir(), "storage")
    tablePath = os.path.join(tablePathBase, tbName+".wtbl")
    # undone
    if os.path.exists(tablePath):
        tmpT = table.Table(tbName)
        with open(tablePath, 'r') as f:
            lines = f.readlines()
            cols = [i.strip().split(",")[0] for i in lines]
            ind = 0
            for c in colNames:
                if c in cols:
                    colFilePath = os.path.join(tablePathBase, c+".wcol")
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
                        colFilePath = os.path.join(tablePathBase, c_+".wcol")
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
        print("table {} not exists".format(tbName))

if __name__ == "__main__":
    pass