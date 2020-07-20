def mergesort(arr):
    if len(arr) > 1:
        mid = len(arr) / 2
        left = arr[:mid]
        right = arr[mid:]
        print "arr", arr, "left", left, "right", right
        mergesort(left)
        print "CHECK1"
        mergesort(right)
        print "CHECK2"
        i = j = k = 0
        while i < len(left) and j < len(right):
            if left[i] < right[j]:
                arr[k] = left[i]
                i = i + 1
            else:
                arr[k] = right[j]
                j = j + 1
            k = k + 1
        while i < len(left):
            arr[k] = left[i]
            i = i + 1
            k = k + 1
        while j < len(right):
            arr[k] = right[j]
            j = j + 1
            k = k + 1
        print "SORTED", arr
    return arr


print mergesort([1, 3, 6, 8, 9, 4, 6, 8])
