import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import json
#import pyspark.sql.types as tp
#from pyspark.ml import Pipeline
#from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler
#from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
#from pyspark.ml.classification import LogisticRegression
#from pyspark.sql import Row

sc=SparkContext('local[2]',appName="crime")
ss=SparkSession(sc)


ssc=StreamingContext(sc,batchDuration=2)
lines=ssc.socketTextStream('localhost',6100)

my_schema=["feature0","feature1","feature2","feature3","feature4","feature5","feature6","feature7","feature8"]


    
def rddtoDf(rdd):
    x = rdd.collect()
    if len(x) > 0:
        y = json.loads(x[0])
        z = y.values()
        df=ss.createDataFrame(data=z,schema=my_schema)
        df_copy=df
        df_copy=df_copy.drop(['feature0'])
        df.show()    
  

lines.foreachRDD(lambda x:rddtoDf(x))

ssc.start()
ssc.awaitTermination()
ssc.stop()
