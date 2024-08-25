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
        return tmpT
    else:
        print("table {} not exists".format(tbName))

if __name__ == "__main__":
    pass