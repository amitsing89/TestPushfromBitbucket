import os

data_dir = "/home/cloudera/Desktop/data"
arr = os.listdir(data_dir)

file_map = {}
for i in arr:
    f = open(data_dir + "/" + i, 'r')
    for j in f.readlines():
        x = j.split("|^")
        if 'FILEHEADER' in j:
            file_map[i] = [x[2], x[3], x[4], x[6]]
        elif '@PUMAIN' in j:
            file_map[i].append(j.strip('\n'))

for val in file_map:
    print val, file_map[val]
