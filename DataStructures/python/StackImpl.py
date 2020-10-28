from Stack import Stack

a = Stack()
for i in range(10):
    a.push("23{0}".format(i))
x = a.container
lengthofstack = len(x)
print lengthofstack
mid = lengthofstack / 2
print x, x[mid]

# x = a.pop()
# print x
