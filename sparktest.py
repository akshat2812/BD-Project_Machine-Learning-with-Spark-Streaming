from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType
import json
import pprint

#spark_sc = spark streaming context

spark_context=SparkContext("local[2]",appName="bd_shiz")
spark_sc=StreamingContext(spark_context, 1)
lines= spark_sc.socketTextStream("localhost",6100)

columns = ['feature0','feature1','feature2']

def check_json(js, col):
    try:
        data = json.loads(js)
        return [data.get(i) for i in col]
    except:
        return []


def convert_json2df(rdd, col):
    ss = SparkSession(rdd.context)
    if rdd.isEmpty():
        return
    df = ss.createDataFrame(rdd, schema=StructType("based on 'col'"))
    df.show()

lines = lines.flatMap(lambda x: check_json(x,columns))
lines = lines.flatMap(lambda x: convert_json2df(x,cols))

#dict_df = json.load(lines)
#dict_df.pprint()

#lines.pprint()


spark_sc.start()
spark_sc.awaitTermination()
spark_sc.stop()


