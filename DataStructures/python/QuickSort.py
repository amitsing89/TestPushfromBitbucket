def quicksort(arr, low, high):
    # print "A", arr
    if low < high:
        # print low, high
        pivot = partition(arr, low, high)
        # print "P", pivot
        quicksort(arr, low, pivot - 1)
        quicksort(arr, pivot + 1, high)
    return arr


def partition(arr, low, high):
    for i in range(low, high):
        print "Pivot", high, arr[high]
        if arr[i] <= arr[high]:
            swap(arr, i, low)
            low += 1
    print "For loop", arr[i], arr[high], "Indexes:", i, high, low
    swap(arr, low, high)
    return low


def swap(arr, low, high):
    print "C", arr
    arr[low], arr[high] = arr[high], arr[low]
    print "AFTER Swap", arr


print quicksort([5, 2, 9, 3, 4, 7], 0, 5)
