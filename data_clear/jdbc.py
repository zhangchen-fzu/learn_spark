from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, IntegerType, StringType

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('test').\
        config('spark.sql.shuffle.partitions', 2).getOrCreate()

    schema = StructType().add('userid', StringType(), nullable=True).\
        add('movieid', IntegerType(), nullable=True).\
        add('score', IntegerType(), nullable=True).\
        add('ts', StringType(), nullable=True)

    df = spark.read.format('csv').schema(schema=schema).\
        option('sep', '\t').option('header', False).option('encoding', 'utf-8').\
        load(r'd:\pyspark\testdata\sql\u.data')

    #写入数据到jdbc中
    df.write.mode('overwrite').format('jdbc').\
        option('url', 'jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true').\
        option('dbtable', 'movie_data').\
        option('user', 'root').option('password', '123').save()

    #读取jdbc数据
    df2 = spark.read.format('jdbc').\
        option('url', 'jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true').\
        option('dbtable', 'movie_data').\
        option('user', 'root').option('password', '123').load()
    df2.printSchema()
    df2.show()