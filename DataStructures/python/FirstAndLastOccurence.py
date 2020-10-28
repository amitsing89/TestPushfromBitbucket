def find(checklist, num):
    first = -1
    last = -1
    for i in range(len(checklist)):
        # print first, last, i, num, checklist[i]
        if num == checklist[i] and first == -1:
            first = i
        if num == checklist[i] and first != -1:
            last = i
    print("FIRST Occurence --->", first, "LAST Occurence --->", last)


find([1, 3, 5, 5, 5, 5, 67, 123, 125, 1, 3, 5, 5, 5, 5, 67, 123, 125], 125)
