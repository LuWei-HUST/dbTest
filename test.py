import table

t = table.Table()

t.addColumn("id")
t.addColumn("val")

t.append(["a", 1, "b", 2, "c", 3])

print(t.colNames)
print(t.t)

rowChar = "-"
colNum = t.colNum
rowLine = rowChar * 8 * colNum + rowChar * (colNum)
# print(rowLine)

for i in range(colNum):
    if i + 1 == colNum:
        print(t.colNames[i], end="")
        print("      ", end=None)
    else:
        print(t.colNames[i], end="")
        print("      ", end="")