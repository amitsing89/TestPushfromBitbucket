# def  compress(str):
#     new_map = {}
#     counter = 1
#     for character in str:
#         if character in new_map.keys():
#             count = new_map[character] + 1
#             new_map[character] = count
#         else:
#             new_map[character]= counter
#     return new_map
#
# x = compress("aaabbbsssyyyrrr")
# retstr = ""
# for i in x:
#     retstr = retstr+str(i)+str(x[i])
# print retstr


def compress(strs):
    return_string = ""
    new_map = {}
    counter = 1
    for character in strs:
        if character in new_map.keys():
            count = new_map[character] + 1
            new_map[character] = count
        else:
            new_map[character] = counter
    dt = sorted(new_map.iteritems())
    for k, v in dt:
        if v > 1:
            return_string = return_string + k + str(v)
        else:
            return_string = return_string + k

    return return_string


print(compress("aaabbbsssyyyrrrzrse"))

# import re
#
# letter_pattern = re.compile("^([a-z]+)+$")
# number_pattern = re.compile("^([0-9]+)+$")
# x = letter_pattern.match('')
# y = number_pattern.match(str(11))
# if x and y:
#     print "T"
# else:
#     print "F"
