from pyspark.sql import SparkSession
from operator import add


# 统计双十一购买了商品的买家年龄段比例


if __name__ == "__main__":


    spark = SparkSession\
        .builder\
        .appName("Task22")\
        .getOrCreate()


    # 读入 log

    log = spark.read.text("user_log_format1.csv").rdd.map(lambda r: r[0])

    # 读入 info

    info = spark.read.text("user_info_format1.csv").rdd.map(lambda r: r[0])


    log2 = log.map(lambda x: x.split(",")).filter(lambda x: x[6] == '2' and x[5] == '1111')
    buyer = log2.map(lambda x:x[0]).distinct().collect()
    
    info2 = info.map(lambda x:x.split(",")).filter(lambda x: (x[0] in buyer) and (x[1]=='1' or x[1]=='2' or x[1]=='3' or x[1]=='4'or x[1]=='5'or x[1]=='6'or x[1]=='7'or x[1]=='8'))
    age = info2.map(lambda x: ((x[1]),1)).reduceByKey(add)
    

    # 输出结果

    result = age.collect()

    for (i, j) in result:

        print(i, j)


    spark.stop()