s = 'abcd'
print(sorted(s))

seq = sorted(s)

for i in range(len(seq)):
    x = seq[:i]+seq[i+1:]
    print(seq[:i] + seq[i+1:])
    # print(x)
#     for j in range(len(sortedval)):
        # print()
# for i in range(len(s)):
#     for j in range()