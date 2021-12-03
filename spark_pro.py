import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
#import pyspark.sql.types as tp
#from pyspark.ml import Pipeline
#from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler
#from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
#from pyspark.ml.classification import LogisticRegression
#from pyspark.sql import Row

sc=SparkContext('local[2]',appName="crime")
ss=SparkSession(sc)

#my_schema=tp.StructType([
  #tp.StructField(name= 'Dates',       dataType= tp.IntegerType(),  nullable= True),
  #tp.StructField(name= 'label',       dataType= tp.IntegerType(),  nullable= True),
  #tp.StructField(name= 'tweet',       dataType= tp.StringType(),   nullable= True)
  


ssc=StreamingContext(sc,batchDuration=2)
lines=ssc.socketTextStream('localhost',6100)
words=lines.flatMap(lambda x: x.split(','))
if lines:
	lines.foreachRDD(lambda x: print(x))
ssc.start()
ssc.awaitTermination()
ssc.stop()
