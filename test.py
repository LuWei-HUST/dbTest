colpath = "/home/luwei/code/dbTest/storage/id.wcol"

with open(colpath, 'r') as f:
    lines = f.readlines()
    lines = [i.strip() for i in lines]
    print(lines)