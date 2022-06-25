from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, ArrayType, StringType, StructType
import string

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').appName('test'). \
        config('spark.sql.shuffle.partitions', 2).getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([1, 2, 3, 4, 5], 3).map(lambda x:[x])
    df = rdd.toDF(['nums'])

    # 将df转换为rdd的形式，利用rdd来模拟udaf
    rdd1 = df.rdd.repartition(1)

    def process(lst):
        total = 0
        for n in lst:
            total += n['nums']
        return [total] #mapPartitions的返回值必须是list对象

    print(rdd1.mapPartitions(process).collect())
