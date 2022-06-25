from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, IntegerType


if __name__ == '__main__':
    spark = SparkSession.builder.appName('test').master('local[*]').\
        config('spark.sql.shuffle.partitions', 2).\
        getOrCreate()
    sc = spark.sparkContext

    schema = StructType(). \
        add('userid', StringType(), nullable=True). \
        add('movieid', IntegerType(), nullable=True). \
        add('score', IntegerType(), nullable=True). \
        add('time', StringType(), nullable=True)
    df = spark.read.format('csv'). \
        option('sep', '\t'). \
        option('header', False). \
        option('encoding', 'utf-8'). \
        schema(schema=schema). \
        load(r'd:\pyspark\testdata\sql\u.data')

    # # TODO 1:查询用户的平均分（SQL版本）
    df.createTempView('movie_table')
    spark.sql('select userid, round(avg(score), 2) as user_avg_score from movie_table group by userid order by user_avg_score desc').show()
    # # TODO 1:查询用户的平均分（DSL版本）
    df.groupBy('userid'). \
        avg('score'). \
        withColumnRenamed('avg(score)', 'user_avg_score'). \
        withColumn('user_avg_score', F.round('user_avg_score', 2)). \
        orderBy('user_avg_score', ascending=False). \
        show()


    # #TODO 2查询电影平均分（SQL版本）
    spark.sql('select movieid, round(avg(score), 2) as movie_avg_score from movie_table group by movieid order by movie_avg_score desc').show()
    # # TODO 2查询电影平均分（DSL版本）
    df.groupBy('movieid').\
        avg('score').\
        withColumnRenamed('avg(score)', 'movie_avg_score').\
        withColumn('movie_avg_score', F.round('movie_avg_score', 2)).\
        orderBy('movie_avg_score', ascending=False).\
        show()

    # #TODO 3:查询大于平均分的电影的数量（SQL版本）
    spark.sql('select count(*) from movie_table where score > (select avg(score) from movie_table)').show()
    # # TODO 3:查询大于平均分的电影的数量（DSL版本）
    print('大于平均分的电影的数量', df.where(df['score'] > df.select(F.avg(df['score'])).first()['avg(score)']).count())

    # #TODO 4:查询评分大于3电影中，打分次数最多的用户，并求出平均分（SQL版本）
    spark.sql('select avg(score) as avg_score from movie_table where userid in (select userid from (select userid, count(*) as u_num from movie_table where score > 3 group by userid order by u_num desc limit 1) as a)').show()
    # # TODO 4:查询评分大于3电影中，打分次数最多的用户，并求出平均分（DSL版本）
    userid = df.where('score > 3').\
        groupBy('userid').\
        count().\
        withColumnRenamed('count', 'u_num').\
        orderBy('u_num', ascending=False).\
        limit(1).first()['userid']
    df.where(df['userid'] == userid).select(F.avg('score')).show()

    # # TODO 5:查询每个用户的平均打分，最低打分，最高达分（SQL版本）
    spark.sql('select ound(avg(score), 2) as avg_score, min(score) as min_score, max(score) as max_score from movie_table group by userid').show()
    # # TODO 5:查询每个用户的平均打分，最低打分，最高达分（DSL版本）
    df.groupBy('userid').agg(F.round(F.avg('score'), 2).alias('avg_score'), F.min('score').alias('min_score'), F.max('score').alias('max_score')).show()

    # TODO 6:查询被评分超过100次的电影，的平均分排名top-10（SQL版本）
    spark.sql('select movieid, avg(score) as m_avg, count(*) as s_cnt from movie_table group by movieid having s_cnt > 100 order by m_avg desc limit 10').show()
    # TODO 6:查询被评分超过100次的电影，的平均分排名top-10（DSL版本）
    df.groupBy('movieid').\
        agg(F.avg('score').alias('avg_score'), F.count('movieid').alias('cnt')).\
        where('cnt > 100').\
        orderBy('avg_score', ascending=False).\
        limit(10).show()