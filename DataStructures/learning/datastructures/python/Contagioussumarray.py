def maxSubArraySum(a,size):

    max_so_far =a[0]
    curr_max = a[0]

    for i in range(1,size):
        # print "Before",max_so_far,curr_max
        curr_max = max(a[i], curr_max + a[i])
        max_so_far = max(max_so_far,curr_max)
        # print "After",max_so_far,curr_max
    return max_so_far



a = [-2, -3, 4, -1, -2, 1, 5, -3]
maxSubArraySum(a,len(a))
