from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import ConfigParser
config = ConfigParser.RawConfigParser()
config.read('parameters.cfg')

from TaasRequestUtil  import TaasRequestUtil

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", config.get('SPARK_APP','app_name'))
ssc = StreamingContext(sc, config.getint('SPARK_APP', 'delay'))

# initiate login request and get the token
taasRequestUtil = TaasRequestUtil()
taasRequestUtil.login()

# Create a DStream source
lines = ssc.socketTextStream(config.get('SPARK_HOST', 'host_name'), config.getint('SPARK_HOST', 'port'))

words = lines.flatMap(lambda line: line.split(" "))
tokenize_pairs = words.map(lambda word: taasRequestUtil.tokenize(word))
tokenize_pairs.pprint()
# Output tokenized pairs
tokenize_pairs.pprint()

# detokenize pairs; just reversing the above output
detokenize_pairs = tokenize_pairs.map(lambda (word, token): taasRequestUtil.detokenize(token))
detokenize_pairs.pprint()


ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate


