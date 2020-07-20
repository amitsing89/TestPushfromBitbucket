def highest_value(data, strs):
    max_val = 0
    min_val = 0
    for i in range(len(data)):
        if data[max_val] < data[i]:
            max_val = i
        elif data[min_val] > data[i]:
            min_val = i
    if 'highest' in strs:
        return data[max_val]
    else:
        return data[min_val]


datastr = [[12, 34, 65], [76, 45, 34], [5, 34, 76], [2, 3, 4, 5, 6, 7, 90]]

empty_list = []
for i in datastr:
    x1 = highest_value(i, "highest")
    empty_list.append(x1)
emp_l = [7, 3, 4, 5, 6, 2, 1, 91, 92, 90, 86]
print(empty_list)


def second_highest_finder(data):
    max_val = 0
    second_max_val = 0
    prev_max = data[1]
    for i in range(0, len(data)):
        if data[max_val] < data[i]:
            second_max_val = data[max_val]
            max_val = i
        elif data[max_val] > data[i]:
            max_val = max_val
            second_max_val = max(prev_max, data[i])
            prev_max = max(prev_max, data[i])
    return data[max_val], second_max_val


max_value = highest_value(empty_list, "lowests")
print(second_highest_finder(emp_l))
