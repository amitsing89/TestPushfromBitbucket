# import collections
# import regex as re
#
# inverted_index = collections.defaultdict(set)
# for line in open("/home/cloudera/Documents/Source/Phase_2/DPE/DPEService/dummydata.csv"
#                  ""):
#     word = line.strip().lower()  # ignore case
#     for letter in word:
#         inverted_index[letter].add(word)
#
# # a.*e.*i.*o.*u.*y
# # sequential_vowel = re.compile("phone_number im")
# # p = re.compile(r"\L<words>", words=['phone_number', 'credit_card', 'consumer', 'month'])
# p = re.compile(r"\L<words>", words=['pho','ND'])
# words = [line.strip() for line in open("/home/cloudera/Documents/Source/Phase_2/DPE/DPEService/dummydata.csv") if
#          p.search(line)]
#
# for i in words:
#     print i
#
#
import collections

# a = collections.OrderedDict()
a = {'b': 3, 'c': 2, 'a': 1}
print (a.items()[0])
