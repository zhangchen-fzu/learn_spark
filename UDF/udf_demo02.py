from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, ArrayType, StringType, StructType
import string

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').appName('test'). \
        config('spark.sql.shuffle.partitions', 2).getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([[1], [2], [3]])
    df = rdd.toDF(['num_dic'])

    def process(data):
        # string.ascii_letters表示所有的小写字母，[data]表示取所有字母中的第data个
        return {'num':data, 'letter':string.ascii_letters[data]}

    schema = StructType().add('num', IntegerType(), nullable=True).add('letter', StringType(), nullable=True)
    #字典作为返回值时，类型描述比较特殊，由于types中没有提供字典类型，所以需要通过StructType来定义
    udf2 = spark.udf.register('udf1', process, schema)
    df.selectExpr('udf1(num_dic)').show(truncate=False)
    df.select(udf2(df['num_dic'])).show(truncate=False)


