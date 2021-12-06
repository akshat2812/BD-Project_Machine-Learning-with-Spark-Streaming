import pyspark
import np
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import json
#import pyspark.sql.types as tp
#from pyspark.ml import Pipeline
from pyspark.ml.feature import LabelEncoder
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression


sc=SparkContext('local[2]',appName="crime")
ss=SparkSession(sc)


ssc=StreamingContext(sc,batchDuration=2)
lines=ssc.socketTextStream('localhost',6100)

my_schema=["feature0","feature1","feature2","feature3","feature4","feature5","feature6","feature7","feature8"]

def label_encoder(df_copy):
    le = LabelEncoder()
    y = le.fit_transform(np.array(df_copy.select('feature2').collect()))
    # print(y)
    tokenizer(df_copy, y)
    
def tokenizer(df, y):
    feature1 = RegexTokenizer(inputCol="feature1", outputCol='tokens1', pattern='\\W')
    feature2 = RegexTokenizer(inputCol="feature2", outputCol='tokens2', pattern='\\W')
    feature3 = RegexTokenizer(inputCol="feature3", outputCol='tokens3', pattern='\\W')
    feature4 = RegexTokenizer(inputCol="feature4", outputCol='tokens4', pattern='\\W')
    feature5 = RegexTokenizer(inputCol="feature5", outputCol='tokens5', pattern='\\W')
      
    tk1 = feature1.transform(df).select('tokens1')
    tk2 = feature2.transform(df).select('tokens2')
    tk3 = feature3.transform(df).select('tokens3')
    tk4 = feature4.transform(df).select('tokens4')
    tk5 = feature5.transform(df).select('tokens5')
    stop_words_remover(tk1, tk2,tk3,tk4,tk5, y)


def stop_words_remover(col1, col2, y):
    feature0 = StopWordsRemover(inputCol='tokens0', outputCol='filtered_words0')
    feature1 = StopWordsRemover(inputCol='tokens1', outputCol='filtered_words1')
    swr0 = feature0.transform(col1).select('filtered_words0')
    swr1 = feature1.transform(col2).select('filtered_words1')
    # word2vec(swr0, swr1, y)
    hash_vectoriser(swr0, swr1, y)
    
def rddtoDf(rdd):
    x = rdd.collect()
    if len(x) > 0:
        y = json.loads(x[0])
        z = y.values()
        df=ss.createDataFrame(data=z,schema=my_schema)
        df_copy=df
        df_copy=df_copy.drop('feature0','feature6')
        df_copy.show()
        
   

lines.foreachRDD(lambda x:rddtoDf(x))

ssc.start()
ssc.awaitTermination()
ssc.stop()
