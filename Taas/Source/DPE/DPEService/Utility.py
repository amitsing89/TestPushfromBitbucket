# import pandas
import arrow
import random
import string

import sys

import itertools


class Utility():
    def __init__(self):
        self.datetimeobject = arrow.now().format('YYYY-MM-DD')
        self.readfile = open(sys.argv[1], 'r+')

    # Random integer generator method
    def random_with_N_digits(self, n):
        range_start = 10 ** (n - 1)
        range_end = (10 ** n) - 1
        return random.randint(range_start, range_end)

    def stringGenerators(self, num):
        char_set = string.ascii_uppercase + string.digits
        key = "name{0}".format(num)
        val = ''.join(random.sample(char_set * 9, 9))
        dictionary = {key: val}
        return dictionary

    # Dummy data creation
    def testDataCreation(self,writefile):
        tdata = "+91-" + str(self.random_with_N_digits(10)) + "," + str(self.random_with_N_digits(16)) + "," + str(
            self.random_with_N_digits(12)) + "," + str(self.random_with_N_digits(3)) + "." + str(
            self.random_with_N_digits(3)) + "." + str(self.random_with_N_digits(2)) + "." + str(
            self.random_with_N_digits(2)) + "," + str(self.random_with_N_digits(3)) + "-" + str(
            self.random_with_N_digits(3)) + "-" + str(self.random_with_N_digits(3)) + "," + self.datetimeobject + "\n"
        # print tdata
        writefile.writelines(tdata)

    # Reading thorugh a file
    def readFromFile(self, batch):
        line_iterator = itertools.islice(self.readfile, batch)
        data_list = []

        for line in line_iterator:
            split_data_list = line.split(',')
            split_data_list = map(lambda s: s.strip(), split_data_list)
            data_list.append(map(lambda s: s.strip(), split_data_list))

        return data_list

    def readHeaderFromFile(self):
        file = self.readfile
        header_list = []

        for lines in file:
            if len(header_list) == 0:
                header_list = lines.split(',')
                # print header_list
                header_list = map(lambda s: s.strip(), header_list)
                # header_list
                break

        return header_list
