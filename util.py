import os
import table

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

if __name__ == "__main__":
    print(getHomeDir())