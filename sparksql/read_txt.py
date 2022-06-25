from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType


if __name__ == '__main__':

    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc = spark.sparkContext

    scheam = StructType().add('data', StringType(), nullable=True)
    ##读取txt文本数据
    df = spark.read.format('text').schema(schema=scheam).load(r'd:\pyspark\testdata\sql\people.txt')
    df.printSchema()
    df.show()

