from LRU import *
from time import sleep


def print_cache(cache):
    for i, item in enumerate(cache.item_list):
        print ("index: {0} "
               "key: {1} "
               "item: {2} "
               "timestamp: {3}".format(i,
                                       item.key,
                                       item.item,
                                       item.timestamp))

one = LRUCacheItem(1, 'one')
two = LRUCacheItem(2, 'two')
three = LRUCacheItem(3, 'three')

print("Initial cache items.")
cache = LRUCache(length=3, delta=5)
cache.insertItem(one)
cache.insertItem(two)
cache.insertItem(three)
print_cache(cache)
print("-" * 200)

print("Insert a existing item: {0}.".format(one.key))
cache.insertItem(one)
print_cache(cache)
print("#" * 200)

print("Insert another existing item: {0}.".format(two.key))
cache.insertItem(two)
print_cache(cache)
print("*" * 200)

print("Validate items after a period of time")
sleep(6)
cache.validateItem()
print_cache(cache)
print("@" * 200)