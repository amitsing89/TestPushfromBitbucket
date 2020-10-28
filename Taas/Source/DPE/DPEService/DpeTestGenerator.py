#import time
import sys
from Utility import Utility

util = Utility()
writefile = open(sys.argv[1], 'a')

while True:
    util.testDataCreation(writefile)
    #time.sleep(1)