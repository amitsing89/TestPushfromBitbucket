from pyspark import SparkContext, SparkConf, SQLContext

conf = SparkConf().setAppName("FileFormatReader")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

ff = sc.newAPIHadoopFile("/sparkdemo/hdfs/sample.avro", "org.apache.avro.mapreduce.AvroKeyInputFormat",
                         "org.apache.avro.mapred.AvroKey",
                         "org.apache.hadoop.io.NullWritable",
                         keyConverter="org.apache.spark.examples.pythonconverters.AvroWrapperToJavaConverter"
                         )

# dataFrame = sqlContext.createDataFrame(ff)
# dataFrame.write.format("com.databricks.spark.avro").save("/sparkdemo/hdfs/")
print ff.collect()
