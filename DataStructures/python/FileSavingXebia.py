import random
import sys


def random_with_N_digits(n):
    range_start = 10 ** (n - 1)
    range_end = (10 ** n) - 1
    return random.randint(range_start, range_end)


def fileSave(n, path):
    f = open(path, 'a')
    print(f)
    for i in range(n):
        # print (str(random_with_N_digits(5)) + "." + str(random_with_N_digits(2)))
        # Writing a combination of 00000.00 number into file
        f.write(str(random_with_N_digits(5)) + "." + str(random_with_N_digits(2)) + "\n")


# Test
# fileSave(sys.argv[1], "/root/check/IdeaProjects/pythonscripts/com.bt.big-data/ra/bigdata/impl/as.txt")

# arguments for number and path
fileSave(sys.argv[1], sys.argv[2])
