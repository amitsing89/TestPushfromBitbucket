import random
import arrow


def random_with_N_digits(n):
    range_start = 10 ** (n - 1)
    range_end = (10 ** n) - 1
    return random.randint(range_start, range_end)


def testDataCreation():
    test_data = {"phone_number": "+91-" + str(random_with_N_digits(10)),
                 "imein": str(random_with_N_digits(16)),
                 "credit_card": str(random_with_N_digits(12)),
                 "ip": str(random_with_N_digits(3)) + "." + str(random_with_N_digits(3)) + "." + str(
                     random_with_N_digits(2)) + "." + str(random_with_N_digits(2)),
                 "ssn_number": str(random_with_N_digits(3)) + "-" + str(
                     random_with_N_digits(3)) + "-" + str(
                     random_with_N_digits(3)),
                 "date": arrow.now().format('YYYY-MM-DD')}
    return test_data
