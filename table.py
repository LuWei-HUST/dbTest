
from prettytable import PrettyTable

class Table:

    def __init__(self, name) -> None:
        self.name = name
        self.t = []
        self.colNum = 0
        self.colNames = []

    def append(self, vals):
        lenVals = len(vals)
        if lenVals % self.colNum == 0:
            i = 0
            for i in range(lenVals):
                self.t[i % self.colNum].append(vals[i])
        else:
            print("column values should be same length")

    def addColumnData(self, ind, vals):
        for v in vals:
            self.t[ind].append(v)

    def addColumn(self, colName):
        self.t.append([])
        self.colNum += 1
        self.colNames.append(colName)

    def showTable(self):
        pt = PrettyTable()
        for i in range(self.colNum):
            pt.add_column(self.colNames[i], self.t[i])

        print(pt)