from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[2]").setAppName("filter")
sc = SparkContext(conf=conf)

filter_data = {"emp_id": '609373628', "index": 2}  # could be fetch from anywhere
lookup = sc.broadcast(filter_data)
lines = sc.textFile('poc.csv')


def format_lines(l):
    lookup_map = lookup.value

    values = l.split(',')
    new_value = ""
    new_key = ""
    for index, value in enumerate(values):
        if index == lookup_map["index"]:
            new_key = value
        else:
            new_value = new_value + " " + value

    return new_key, new_value


def filter_key(pair):
    k, v = pair
    lookup_map = lookup.value
    # print k , v
    if k == lookup_map["emp_id"]:
        return False
    else:
        return True


out = lines.map(lambda l: format_lines(l)).filter(lambda pair: filter_key(pair))
print out.collect()

out.saveAsTextFile('out')
