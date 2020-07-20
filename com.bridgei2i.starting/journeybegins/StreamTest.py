from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = "--master local[2] --jars C:\\Users\\amitkumar.singh\\Downloads\\spark-streaming-kafka-0-8-assembly_2.11-2.3.2.jar pyspark-shell"
sc = SparkContext()
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 60)

kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'my-group', {'parsed_recipes': 1})
parsed = kafkaStream.map(lambda v: json.loads(v[1]))
parsed.count().map(lambda x: 'Tweets in this batch: %s' % x).pprint()

ssc.start()
ssc.awaitTermination()
