from datetime import datetime
import pyspark
import numpy as np
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import json
from sklearn.feature_extraction.text import HashingVectorizer
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer
from pyspark.sql.types import StringType
from pyspark.ml.feature import LabelEncoder
from pyspark.sql.functions import concat_ws
from sklearn.model_selection import train_test_split
from pyspark.sql.functions import col
from sklearn.naive_bayes import BernoulliNB
from sklearn.linear_model import SGDClassifier
from pyspark.sql.types import *



sc=SparkContext('local[2]',appName="crime")
ss=SparkSession(sc)


ssc=StreamingContext(sc,5)
bnb=BernoulliNB()
sgd=SGDClassifier()
lines=ssc.socketTextStream('localhost',6100)

my_schema=StructType([
    StructField("feature0",StringType(),True),
    StructField("feature1",StringType(),True),
    StructField("feature2",StringType(),True),
    StructField("feature3",StringType(),True),
    StructField("feature4",StringType(),True),
    StructField("feature5",StringType(),True),
    StructField("feature6",StringType(),True),
    StructField("feature7",DoubleType(),True),
    StructField("feature8",DoubleType(),True),
])

def label_encoder(df_copy):
    le = preprocessing.LabelEncoder()
    y = le.fit_transform(np.array(df_copy.select('feature5').collect()))
    tokenizer(df_copy, y)
    
def tokenizer(df_copy, y):
    feature1 = RegexTokenizer(inputCol="feature1", outputCol='tokens1', pattern='\\W')
    feature2 = RegexTokenizer(inputCol="feature2", outputCol='tokens2', pattern='\\W')
    feature5 = RegexTokenizer(inputCol="feature5", outputCol='tokens5', pattern='\\W')
      
    tk1 = feature1.transform(df_copy).select('tokens1')
    tk2 = feature2.transform(df_copy).select('tokens2')
    tk5 = feature5.transform(df_copy).select('tokens5')
    stop_words_remover(tk2,y,df_copy)


def stop_words_remover(ft2,y,df_copy):
    
    feature2 = StopWordsRemover(inputCol='tokens2', outputCol='filtered_words2')
    swr2 = feature2.transform(ft2).select('filtered_words2')
    
    hash_vec(swr2, y,df_copy)

def hash_vec(swr2,y,df_copy):   
    h2=np.array(swr2.withColumn("filtered_words2",concat_ws(" ","filtered_words2")).collect()).tolist()
    h22=[k[0] for k in h2 ]
    hv_2=HashingVectorizer(alternate_sign=False)
    hv_22=hv_2.fit_transform(h22).toarray()
    
    h1=np.array(df_copy.select("feature1").collect()).tolist()
    h11=[k[0] for k in h1]
    hv_1=HashingVectorizer(alternate_sign=False)
    hv_11=hv_1.fit_transform(h11).toarray()
    
    h3=np.array(df_copy.select("feature3").collect()).tolist()
    h33=[k[0] for k in h3]
    hv_3=HashingVectorizer(alternate_sign=False)
    hv_33=hv_3.fit_transform(h33).toarray()
    
    h4=np.array(df_copy.select("feature4").collect()).tolist()
    h44=[k[0] for k in h4]
    hv_4=HashingVectorizer(alternate_sign=False)
    hv_44=hv_4.fit_transform(h44).toarray()
    
    h5=np.array(df_copy.select("feature5").collect()).tolist()
    h55=[k[0] for k in h5]
    hv_5=HashingVectorizer(alternate_sign=False)
    hv_55=hv_5.fit_transform(h55).toarray()
    
    h7=np.array(df_copy.select("feature7").collect()).tolist()
    h77=[k[0] for k in h7]
    hv_7=HashingVectorizer(alternate_sign=False)
    hv_77=hv_7.fit_transform(h77).toarray()  
    
    h8=np.array(df_copy.select("feature8").collect()).tolist()
    h88=[k[0] for k in h8]
    hv_8=HashingVectorizer(alternate_sign=False)
    hv_88=hv_8.fit_transform(h88).toarray()
    
    x=np.concatenate((hv_11,hv_22,hv_33,hv_44,hv_77,hv_88),axis=1)
    x_train,x_test,y_train,y_test=train_test_split(x,y,test_size=0.3)
    bnb.partial_fit(x_train, y_train, classes=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16])
    print("bnb train accuracy:",bnb.score(x_test, y_test))
    
    sgd.partial_fit(x_train,y_train,classes=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16])
    print("SGD Accuracy:",sgd.score(x_test,y_test) )
    
def rddtoDf(rdd):
    x = rdd.collect()
    if len(x) > 0:
        y = json.loads(x[0])
        z = y.values()
        df=ss.createDataFrame(data=z,schema=my_schema)
        df_copy=df
        df_copy=df_copy.drop('feature0','feature6')
        df_copy=df_copy.withColumn("feature7",col("feature7").cast(StringType()))
        df_copy=df_copy.withColumn("feature8",col("feature8").cast(StringType()))
        label_encoder(df_copy)
        
lines.foreachRDD(lambda x:rddtoDf(x))


ssc.start()
ssc.awaitTermination()
ssc.stop()
