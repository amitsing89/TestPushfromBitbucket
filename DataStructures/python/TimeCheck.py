import threading
import time
import sys


def timeCheck():
    start_time = time.clock()
    for i in range(10 ** 10, 10 ** 11):
        if i % 7 == 0:
            print i
    end_time = time.clock() - start_time
    print end_time


def threadTimeCheck(startlimit, endlimit):
    start_time = time.clock()
    for i in xrange(startlimit, endlimit):
        if i % 7 == 0:
            x = i
            # print i
    end_time = time.clock() - start_time
    print end_time


x = 10 ** 6
start_time = time.clock()
for i in range(0, int(sys.argv[1])):
    step = 10 ** 8 / int(sys.argv[1])
    check_time = threading.Thread(target=threadTimeCheck,
                                  kwargs={'startlimit': x + (step * i), 'endlimit': x + step * (i + 1)})
    check_time.start()
    print "THREAD"
    # check_time.join()

end_time = time.clock() - start_time

print end_time
