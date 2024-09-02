import os
import table
import struct

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

if __name__ == "__main__":
    print(getHomeDir())