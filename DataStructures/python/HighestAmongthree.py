# a = 20
# b = 80
# c = 64
# if a > b and a > c:
#     print("Highest is A")
# elif b > a and b > c:
#     print ("Highest is B")
# else:
#     print ("Highest is C")

a = [20, 80, 66, 90, 64, 2, 3]
max_index = 0
for i in range(len(a)):
    if a[max_index] > a[i]:
        max_index = max_index
    else:
        max_index = i
print ("Highest Number", a[max_index])
