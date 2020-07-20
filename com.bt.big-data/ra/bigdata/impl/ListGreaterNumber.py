# [1,11,-3,6]


# def recr(li):
#     for i in range(len(li)):


# l = [1, -50, -67, -21, 45, 11, -3, 6]
# for i in range(len(l)):
#     for j in range(i + 1, len(l)):
#         if l[i] < l[j]:
#             print l[j]
#             break
#         else :
#             x = 1
#             recr(l)
#             else:
#                 print "-1"
#             if not l[i] < l[j]:
#                 print "-1"


def maxSubArray(ls):
    if len(ls) == 0:
        raise Exception("Array empty")  # should be non-empty

    runSum = maxSum = ls[0]
    i = 0
    start = finish = 0

    for j in range(1, len(ls)):
        if ls[j] > (runSum + ls[j]):
            runSum = ls[j]
            i = j
        else:
            runSum += ls[j]

        if runSum > maxSum:
            maxSum = runSum
            start = i
            finish = j

    print ("maxSum =>", maxSum)
    print ("start =>", start, "; finish =>", finish)


# print maxSubArray([-2, 11, -4, 13, -5, 2])
# print maxSubArray([-15, 29, -36, 3, -22, 11, 19, -5])
# print maxSubArray(([1, 1, 1]))
