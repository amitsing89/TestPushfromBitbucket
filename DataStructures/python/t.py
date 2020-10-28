a=8
def f(a):
    a=a+1

b=[1]
def fl(a=None):
    print(a)


b=[1]
def fl1(a=[]):
    print(a)


fl1(b)

fl1()
