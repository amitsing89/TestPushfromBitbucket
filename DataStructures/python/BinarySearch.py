# def binarysearch(arr, l, h, x):
#     if len(arr) > 1:
#         mid = len(arr) / 2
#         left = arr[:mid]
#         right = arr[mid:]
#         if arr[mid] == x:
#             return 1
#
#         elif arr[mid] < x:
#             binarysearch(right, mid, len(right), x)
#             return "founded-" + str(x)
#         else:
#             binarysearch(left, 0, len(left), x)
#             return "found-" + str(x)
#         return "Nothing Found"
#
#
# a = binarysearch([1, 3, 6, 8, 9], 0, 5, 2)
# print a

def binarySearch(arr, vals):
    mid = 0
    if len(arr) > 1:
        mid = len(arr) / 2
        left = arr[:mid]
        right = arr[mid:]
        if vals == arr[mid]:
            print "Found at middle"
            return mid
        elif vals < arr[mid]:
            return binarySearch(left, vals)
        elif vals > arr[mid]:
            return binarySearch(right, vals)
    if mid != 0:
        return arr[mid]
    else:
        return -1


print binarySearch([1, 3, 4, 6, 6, 8, 8, 9], 6)
