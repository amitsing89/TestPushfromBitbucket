for i in range(0, 7):
    for j in range(7, 0, -1):
        if j <= i:
            print j,
        else:
            print " ",
    print ""
for k in range(7, 0, -1):
    for l in range(1, k):
        if l <= k:
            print l,
        else:
            print " ",
    print " "
