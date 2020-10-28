str_a = "aabaabbbbccccdobecodebanc"
check_str = "abc"
map_test = {}
minimum_length = []
for i in range(len(str_a)):
    if str_a[i] in check_str:
        map_test[str_a[i]] = i
        if str_a[i] in map_test.keys() and len(map_test.keys()) == len(check_str):
            max_val = max(map_test.values())
            min_val = min(map_test.values())
            substring = str_a[min_val:max_val + 1]
            minimum_length.append(substring)

print minimum_length
print min(minimum_length, key=len)
