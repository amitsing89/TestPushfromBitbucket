"""
Instructions to candidate.
  1) Given a list of words, group them by anagrams
     Input: ['cat', 'dog', 'god']
     Output: [{'cat'}, {'dog', 'god'}]
  2) Run this code in the REPL to observe its behaviour. The
   execution entry point is main().
  3) Consider adding some additional tests in doTestsPass().
  4) If time permits, some possible follow-ups.
"""
from cassandra.util import sortedset

"""
Returns a list of sets of anagrams

Args:
    words - list of words to process

Example:
    Input: ['cat', 'dog', 'god']
    Output: [{'cat'}, {'dog', 'god'}]
"""
from collections import defaultdict

inp = ['act','odg', 'cat', 'dog', 'god', 'cat']
anagram_dict = {}
# bool_val = 1
for i in range(len(inp)):
    for j in range(i + 1, len(inp)):
        if ''.join(sorted(inp[i])) == ''.join(sorted(inp[j])):
            if str(''.join(sorted(inp[j]))) in anagram_dict.keys():
                anagram_dict[str(''.join(sorted(inp[i])))].add(inp[j])
        else:
            anagram_dict.setdefault(str(''.join(sorted(inp[i]))), {inp[i]})

print(anagram_dict.values())
# print i,bool_val
# if ''.join(sorted(inp[i])) == ''.join(sorted(inp[bool_val])):
#     if ''.join(sorted(inp[i])) in anagram_dict.keys():
#         anagram_dict[''.join(sorted(inp[i]))].add(i)
#         bool_val += 1
# else:
#     print str(''.join(sorted(inp[i])))
#     print [inp[i]]
#     bool_val += 1
#     anagram_dict.setdefault(str(''.join(sorted(inp[i]))), {inp[i]})
# print anagram_dict

# for i in inp:
#     for j in range(1, len(inp)):
#         if ''.join(sorted(i)) == ''.join(sorted(inp[j])):
#             if str(''.join(sorted(i))) not in anagram_dict.keys():
#                 anagram_dict[str(''.join(sorted(i)))] = [i, inp[j]]
#                 bool_val = 1
#     if bool_val == 0:
#         anagram_dict[i] = [i]
#
# print anagram_dict
