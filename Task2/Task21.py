from pyspark.sql import SparkSession
from operator import add


# 统计双十一购买了商品的男女比例


if __name__ == "__main__":


    spark = SparkSession\
        .builder\
        .appName("Task21")\
        .getOrCreate()


    # 读入 log

    log = spark.read.text("user_log_format1.csv").rdd.map(lambda r: r[0])

    # 读入 info

    info = spark.read.text("user_info_format1.csv").rdd.map(lambda r: r[0])


    log2 = log.map(lambda x:x.split(",")).filter(lambda x:x[6] == '2' and x[5] == '1111')
    buyer = log2.map(lambda x:x[0]).distinct().collect()

    info2 = info.map(lambda x:x.split(",")).filter(lambda x:(x[0] in buyer) and (x[2] == '0' or x[2] == '1'))
    gender = info2.map(lambda x:((x[2]),1)).reduceByKey(add)


    # 输出结果

    result = gender.collect()

    for (i, j) in result:

        print(i, j)


    spark.stop()