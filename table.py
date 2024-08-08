
class Table:

    def __init__(self) -> None:
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

    def addColumn(self, colName):
        self.t.append([])
        self.colNum += 1
        self.colNames.append(colName)

    def showTable(self):
        pass