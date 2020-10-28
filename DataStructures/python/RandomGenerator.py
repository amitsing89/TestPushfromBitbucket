import random
import datetime


def random_with_N_digits(n):
    range_start = 10 ** (n - 1)
    range_end = (10 ** n) - 1
    return random.randint(range_start, range_end)


def testDataCreation():
    tdata = "+91-" + str(random_with_N_digits(10)) + "," + str(random_with_N_digits(16)) + "," + str(
        random_with_N_digits(12)) + "," + str(random_with_N_digits(3)) + "." + str(
        random_with_N_digits(3)) + "." + str(random_with_N_digits(2)) + "." + str(
        random_with_N_digits(2)) + "," + str(random_with_N_digits(3)) + "-" + str(
        random_with_N_digits(3)) + "-" + str(random_with_N_digits(3)) + "," + datetime.datetime.now().strftime(
        '%d-%m-%Y') + "\n"
    return tdata


for i in range(10):
    x = testDataCreation()
    print x
