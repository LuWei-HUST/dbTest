import os
import re
import shutil
import table
import copy
import util
from parser import SqlParser

tablePathBase = "/home/luwei/code/dbTest/storage"
curTable = "default"
tDict = {}

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

        