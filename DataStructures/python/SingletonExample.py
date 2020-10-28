def singleton(cls):
    instances = {}

    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]

    return getinstance


class Counter:
    def __init__(self):
        self.count = 0

    def inc(self):
        self.count += 1


# print(type(Counter))
# x = Counter()
# print(x.inc())
# Counter = singleton(Counter)
# print(type(Counter))


print(abs(5-2))