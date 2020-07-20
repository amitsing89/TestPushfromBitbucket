def reverseString(str_val):
    return str_val[::-1]


def get_all_substrings(string_val):
    emp_list = set()
    for i in range(len(string_val)):
        for j in range(len(string_val), i, -1):
            if string_val[i:j] == reverseString(string_val[i:j]) and len(string_val[i:j]) > 1:
                emp_list.add(string_val[i:j])
    return emp_list


x = get_all_substrings("racecarenterelephantmalayalam")
print (x)


def matrixSolution(x):
    pass

# val_pass = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
# print val_pass[0][1]
# for i in range(len(val_pass)):
#     for j in range(len(val_pass[i])):
#            print i, j
