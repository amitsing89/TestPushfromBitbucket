class LRU():
    def __init__(self, limit):
        self._lru_list = []
        self.lru_hashmap = {}
        self.limit = limit

    def set(self, key, val):
        self.lru_hashmap[key] = val
        if len(self._lru_list) <= self.limit and key not in self._lru_list:
            self._lru_list.append(key)
        elif key in self._lru_list:
            index_value = self._lru_list.index(key)
            self._lru_list.pop(index_value)
            self._lru_list.insert(self.limit, key)
        elif len(self.lru_hashmap) > self.limit:
            self._lru_list.pop(0)
            self._lru_list.insert(self.limit, key)

    def get(self, key):
        return self.lru_hashmap[key]

    @property
    def lru_list(self):
        return self._lru_list


if __name__ == "__main__":
    a = LRU(6)
    for i in range(0, 9):
        a.set('amit{0}'.format(i), 'singh')
        print a.lru_list  # , a.lru_hashmap
    for j in range(6, 17):
        a.set('amit{0}'.format(j), 'singh')
        print a.lru_list  # , a.lru_hashmap
    for k in range(0, 7):
        a.set('amit{0}'.format(k), 'singh')
        print a.lru_list  # , a.lru_hashmap
