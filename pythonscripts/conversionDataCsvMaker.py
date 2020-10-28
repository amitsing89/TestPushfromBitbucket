import json
import datetime
import csv
import sys

directory = "/opt/amit_scripts_python/{0}".format(sys.argv[1])
print directory
f = open(directory, 'r')

f1 = open(sys.argv[1] + '.csv', 'w')
csv_file = csv.writer(f1)

for lines in f.readlines():
    convJson = json.loads(lines)
    attributedVal = convJson['billingAttribution']
    l = []
    for item in attributedVal:
        # csv_file.writerow(attributedVal[item])
        l.append(attributedVal[item])
    # csv_file.writerow([item,attributedVal[item]])
    csv_file.writerow(l)
    l = []
