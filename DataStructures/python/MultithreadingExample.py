# import threading
#
# class SummingThread(threading.Thread):
#     def __init__(self,low,high):
#         super(SummingThread, self).__init__()
#         self.low=low
#         self.high=high
#         self.total=0
#
#     def run(self):
#         for i in range(self.low,self.high):
#             self.total+=i
#             print self.total
#
#
# thread1 = SummingThread(0,500000)
# thread2 = SummingThread(500000,1000000)
# thread1.start() # This actually causes the thread to run
# thread2.start()
# thread1.join()  # This waits until the thread has completed
# thread2.join()
# # At this point, both threads have completed
# result = thread1.total + thread2.total
# print "TOTAL",result

from multiprocessing import Process

import threading

lock = threading.Lock()
cnd = threading.Condition()
print lock.acquire()
print lock.acquire(0)


def f(name):
    print 'hello', name


if __name__ == '__main__':
    for i in range(1, 10):
        p = Process(target=f, args=('bob{0}'.format(str(i)),))
        p.start()
        print "NAME", p.name
        print "TIME", i
        p.join()
