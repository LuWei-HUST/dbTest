import re


string = "select * from test;"
string = "select col1, col2 from test;"
print(string)

select_pat = r"[ ]*select[ ]+([1-9a-zA-Z,_ \*]+)[ ]+from[ ]+([1-9a-zA-Z_\*]+)[ ]*;"
res = re.search(select_pat, string)

if res:
    # print(res.group(0))
    colNamesText = res.group(1)
    colNames = colNamesText.strip().split(",")
    colNames = [i.strip() for i in colNames]
    print(colNames)
    tbName = res.group(2)
    print(tbName)