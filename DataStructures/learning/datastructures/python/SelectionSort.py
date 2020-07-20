def selection(arr):
    for i in range(0, len(arr)):
        min = i
        for j in range(i + 1, len(arr)):
            if arr[j] < arr[min]:
                min = j
            print i, j, arr
        temp = arr[min]
        arr[min] = arr[i]
        arr[i] = temp
        print "swap,arr", arr

    return arr


print selection([9, 1, 6, 3, 8])
