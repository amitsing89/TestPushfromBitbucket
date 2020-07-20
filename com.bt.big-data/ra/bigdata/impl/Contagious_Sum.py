def summationReturn(a):
    summation = 0
    for i in range(len(a)):
        for j in range(i, len(a)):
            for k in range(i, j + 1):
                summation = summation + a[k]
                print(summation)

    return summation


list_check = [1, 2, 3, 4, 5]
x = summationReturn(list_check)
print(x)
