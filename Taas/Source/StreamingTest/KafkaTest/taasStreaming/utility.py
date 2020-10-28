import random
# import pandas
import arrow


class Utility():
    def __init__(self):
        self.datetimeobject = arrow.now().format('YYYY-MM-DD')
        self.filedata = open('test.csv', 'rb')

    # Random integer generator method
    def random_with_N_digits(self, n):
        range_start = 10 ** (n - 1)
        range_end = (10 ** n) - 1
        return random.randint(range_start, range_end)

    # Dummy data creation
    def testDataCreation(self):
        test_data = {"phone_number": "+91-" + str(self.random_with_N_digits(10)),
                     "imein": str(self.random_with_N_digits(16)),
                     "credit_card": str(self.random_with_N_digits(12)),
                     "ip": str(self.random_with_N_digits(3)) + "." + str(self.random_with_N_digits(3)) + "." + str(
                         self.random_with_N_digits(2)) + "." + str(self.random_with_N_digits(2)),
                     "ssn_number": str(self.random_with_N_digits(3)) + "-" + str(
                         self.random_with_N_digits(3)) + "-" + str(
                         self.random_with_N_digits(3)),
                     "date": self.datetimeobject}
        return test_data

    # Reading thorugh a file
    def readFromFile(self):
        file = self.filedata
        header_list = []
        data_list = []
        for lines in file:
            if len(header_list) == 0:
                header_list = lines.split(',')
                print header_list
                header_list = map(lambda s: s.strip(), header_list)
                header_list
            else:
                split_data_list = lines.split(',')
                split_data_list = map(lambda s: s.strip(), split_data_list)
                data_list.append(map(lambda s: s.strip(), split_data_list))
        # data = pandas.DataFrame(data_list, columns=header_list)
        # return data
