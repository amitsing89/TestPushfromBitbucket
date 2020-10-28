def checkPattern(x, pattern_seq1):
    j = 0
    k = set()
    for i in range(len(x)):
        # print k,x[i],j,i
        if x[i] == pattern_seq1[0]:
            j = 1
            k = set()
        elif x[i] in k:
            j = 0
        elif x[i] == pattern_seq1[j]:
            k.add(x[i])
            j += 1
        if j == len(pattern_seq1):
            return "Found"


list_check = [5657718727283781872726387610987828176727376767456, 56577187272837281872726387610987828767737676745]

pattern1 = '12345'
pattern2 = '321'

for i in list_check:
    print checkPattern(str(i), pattern1)
    print checkPattern(str(i), pattern2)
