def bubble(arr):
    for i in range(len(arr)):
        for j in range(len(arr) - i - 1):
            if arr[j] > arr[j + 1]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
            print i, j, arr
    return arr


print bubble([2, 3, 6, 9, 1])
