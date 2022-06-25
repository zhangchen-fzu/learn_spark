from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc = spark.sparkContext

    ##parquet数据同json一样，也是有格式的
    df = spark.read.format('parquet').load(r'd:\pyspark\testdata\sql\users.parquet')
    df.printSchema()
    df.show()
