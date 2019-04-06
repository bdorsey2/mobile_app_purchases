from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys

print(sys.argv[1])

sc = SparkContext.getOrCreate()
ss = SparkSession.builder.getOrCreate()

attrdd = sc.textFile(sys.argv[1])

attrdd = attrdd.map(lambda x: [x.split(',')[0],x.split(',')[1],x.split(',')[2],','.join(x.split(',')[3:-1]).replace("\"","").replace("\\",""),x.split(',')[-1]])

header = attrdd.first()

fields = [StructField(field_name, StringType(), True) for field_name in header]
schema = StructType(fields)

attrdd = attrdd.filter(lambda x: 'app_id' not in x)  # rdd.subtract not working, dunno why

attdf = ss.createDataFrame(attrdd, schema =schema)

attdf.show(24)