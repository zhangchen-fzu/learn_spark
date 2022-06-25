from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':

    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile(r'd:\pyspark\testdata\sql\people.txt')
    rdd1 = rdd.map(lambda x:x.split(',')).map(lambda x:(x[0], int(x[1])))

    ##通过StructType创建表结构，可以指定列的数据类型，以及列是否为空
    schema = StructType().add('name', StringType(), True).add('age', IntegerType(), False)
    ##将rdd转化为DataFrame
    df = spark.createDataFrame(rdd1, schema=schema)
    df.printSchema()
    df.show()



