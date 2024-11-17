import util
from parser import SqlParser

if __name__ == "__main__":

    sqlparser = SqlParser()

    while True:
        ch = input("wsql#: ")

        if ch == "quit":
            exit()
        
        if ch == "show tables":
            allTables = util.getAllTables()
            allTables.showTable()
            continue

        sqlparser.parse(ch)

        