#!/usr/bin/python

# Date: 2017-12-30
#
# Description:
# Write a function that, given two integers A and B, returns a string containing
# exactly A letters 'a' and exactly B letters 'b' with no three consecutive
# letters being the same (in other words, neither "aaa" nor "bbb" may occur in
# the returned string).
#
# Examples:
#
# 1. Given A = 5 and a = 3, your function may return "aabaabab'. Note that
# "abaabbaa" would also be a correct answer. Your function may return any
# correct answer.
#
# 2. Given A = 3 and a = 3, your function should return "ababab", "aababb",
# "bbaabb" or any of several other strings.
#
# 3. Given A = 1 and a = 4, your function should return "bbabb", which is the
# only correct answer in this case.
#
# Assume that:
#
# A and a are integers within the range [0..1001;
# at least one solution exists for the given A and B.
#
# Implementation:
# Append aab or bba based on which is greater A or B. Later on there will be
# point when both A and B will be same, then add aabb or bbaa based on just
# previous char in list.
#
# Complexity:
# O(A + B)


def three_non_consecutive_ab(A, B):
    """
    Returns a string with 'a' and 'b' with no 3 same char repeating in sequence.
    Keyword arguments:
    A: Number of 'a' required.
    B: Number of 'b' required.
    """
    result = []

    # set_of_4 will be used when both and A and B becomes equal so to avoid 3 same
    # chars we are creating this based to A and B.
    if A > B:
        set_of_4 = ['a', 'a', 'b', 'b']
    else:
        set_of_4 = ['b', 'b', 'a', 'a']

    while A or B:
        if A > 0 and B > 0:
            if A > B:
                current = ['a', 'a', 'b']
                A -= 2
                B -= 1
            elif A < B:
                current = ['b', 'b', 'a']
                A -= 1
                B -= 2
            else:
                if A > 1:  # Or B > 1 as both A and B are same.
                    current = set_of_4
                    A -= 2
                    B -= 2
                else:
                    current = ['a', 'b']
                    A -= 1
                    B -= 1
        else:
            if A:
                current = ['a'] * A
                A = 0
            else:
                current = ['b'] * B
                B = 0

        result.extend(current)

    return ''.join(result)


def main():
    A = int(input('Input A: '))
    B = int(input('Input B: '))
    print('A: {A}, B: {B}, String: {String}'.format(
        A=A, B=B, String=three_non_consecutive_ab(A, B)))


if __name__ == '__main__':
    main()