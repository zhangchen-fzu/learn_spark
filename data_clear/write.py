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

    #写入数据到text文本中，因为只能写入一列，所以需要提前将数据处理为一列的形式
    df.select(F.concat_ws("---", 'userid', 'movieid', 'score', 'ts')).\
        write.mode('overwrite').format('text').save(r'd:\pyspark\out\text')

    #写入数据到csv文件中，需要处理数据的分隔方式以及数据是否带有表头信息
    df.write.mode('overwrite').format('csv').\
        option('sep', ';').option('header', True).\
        save(r'd:\pyspark\out\csv')

    #写入数据到json文件中，无需对数据做处理
    df.write.mode('overwrite').format('json').save(r'd:\pyspark\out\json')

    # 写入数据到parquet文件中，无需对数据做处理
    df.write.mode('overwrite').format('parquet').save(r'd:\pyspark\out\parquet')

