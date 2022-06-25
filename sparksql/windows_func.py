from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, ArrayType, StringType, StructType
import string

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').appName('test'). \
        config('spark.sql.shuffle.partitions', 2).getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([
        ('张三', 'class_1', 99),
        ('王五', 'class_2', 90),
        ('李四', 'class_3', 88),
        ('决定', 'class_1', 90),
        ('的二', 'class_2', 54),
        ('儿科', 'class_3', 33)
    ])

    schema = StructType().add('name', StringType(), nullable=True).\
        add('class', StringType(), nullable=True).\
        add('score', IntegerType(), nullable=True)
    df = rdd.toDF(schema)

    df.createTempView('stu')
    spark.sql('select *, avg(score) over() as avg_score from stu').show()
    spark.sql('select *, rank() over(order by score desc) as rank1, dense_rank() over(order by score desc) as rank2 from stu').show()