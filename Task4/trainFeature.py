import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit


# 注释部分是因为已经完成 所以不再重复



if __name__ == "__main__":
 

    spark = SparkSession\
        .builder\
        .appName("TrainFeature")\
        .getOrCreate()

    # train = spark.read.option("header",True).csv("train_format1.csv")   
    # test = spark.read.option("header",True).csv("test_format1.csv")    
    # info = spark.read.option("header",True).csv("user_info_format1.csv")   
    # info = spark.read.option("header",True).csv("user_info_preprocess1.csv")    # 读入处理过缺失值的info
    log = spark.read.option("header",True).csv("user_log_format1.csv")


    # -------------------- 第一阶段 -------------------- #

    # 处理缺失值 得到 user_info_preprocess1.csv  train 加上 age gender 和 logs

    # info = info.withColumn('age_range', when(info.age_range.isNull(), lit('0')).otherwise(info.age_range))
    # info = info.withColumn('gender', when(info.gender.isNull(), lit('2')).otherwise(info.gender))
    # info.repartition(1).write.csv("info_preprocess", encoding="utf-8", header=True)

    # train 加上 age_range 和 gender

    # train = train.join(info, on="user_id", how="left")   

    # train 加上 logs

    # logs_count = log.groupby("user_id","seller_id").count()
    # logs_count = logs_count.withColumnRenamed("seller_id","merchant_id")
    # logs_count = logs_count.withColumnRenamed("count","logs")
    # train = train.join(logs_count,on=["user_id","merchant_id"], how="left")
    # train.repartition(1).write.csv("train_add_logs", encoding="utf-8", header=True)

    # 第一阶段完成 得到 train2.csv


    # -------------------- 第二阶段 -------------------- #

    # train 加上 items

    # items_count = log.groupby("user_id","seller_id","item_id").count()
    # items_count = items_count.drop("count") 
    # items_count1 = items_count.groupby("user_id","seller_id").count()
    # items_count1 = items_count1.withColumnRenamed("seller_id","merchant_id")
    # items_count1 = items_count1.withColumnRenamed("count","items")

    # train = spark.read.option("header",True).csv("train2.csv")   
    # train = train.join(items_count1,on=["user_id","merchant_id"], how="left")

    # train.repartition(1).write.csv("train_add_items", encoding="utf-8", header=True)

    # 第二阶段完成 得到 train3.csv

    
    # -------------------- 第三阶段 -------------------- #

    # train 加上 cate

    # cate_count = log.groupby("user_id","seller_id","cat_id").count()
    # cate_count = cate_count.drop("count") 
    # cate_count2 = cate_count.groupby("user_id","seller_id").count()
    # cate_count2 = cate_count2.withColumnRenamed("seller_id","merchant_id")
    # cate_count2 = cate_count2.withColumnRenamed("count","categories")

    # train = spark.read.option("header",True).csv("train3.csv")   
    # train = train.join(cate_count2,on=["user_id","merchant_id"], how="left")

    # train.repartition(1).write.csv("train_add_cate", encoding="utf-8", header=True)

    # 第三阶段完成 得到 train4.csv


    # -------------------- 第四阶段 -------------------- #

    # train 加上 browse

    # browse_count = log.groupby("user_id","seller_id","time_stamp").count()
    # browse_count = browse_count.drop("count") 

    # browse_count1 = browse_count.groupby("user_id","seller_id").count()

    # browse_count1 = browse_count1.withColumnRenamed("seller_id","merchant_id")
    # browse_count1 = browse_count1.withColumnRenamed("count","browse")


    # train = spark.read.option("header",True).csv("train4.csv")   
    # train = train.join(browse_count1,on=["user_id","merchant_id"], how="left")

    # train.repartition(1).write.csv("train_add_browse_days", encoding="utf-8", header=True)

    # 第四阶段完成 得到 train5.csv


    # -------------------- 第五阶段 -------------------- #

    # train 加上 one_clicks shopping_carts purchase_times favourite_times

    clicks_count = log.filter("action_type == 0")
    clicks_count = clicks_count.groupby("user_id","seller_id").count()
    clicks_count = clicks_count.withColumnRenamed("seller_id","merchant_id")
    clicks_count = clicks_count.withColumnRenamed("count","clicks")

    carts_count = log.filter("action_type == 1")
    carts_count = carts_count.groupby("user_id","seller_id").count()
    carts_count = carts_count.withColumnRenamed("seller_id","merchant_id")
    carts_count = carts_count.withColumnRenamed("count","carts")

    purchase_count = log.filter("action_type == 2")
    purchase_count = purchase_count.groupby("user_id","seller_id").count()
    purchase_count = purchase_count.withColumnRenamed("seller_id","merchant_id")
    purchase_count = purchase_count.withColumnRenamed("count","purchase")

    favorite_count = log.filter("action_type == 3")
    favorite_count = favorite_count.groupby("user_id","seller_id").count()
    favorite_count = favorite_count.withColumnRenamed("seller_id","merchant_id")
    favorite_count = favorite_count.withColumnRenamed("count","favorite")


    train = spark.read.option("header",True).csv("train5.csv") 

    train = train.join(clicks_count,on=["user_id","merchant_id"], how="left")
    train = train.withColumn("clicks", when(train.clicks.isNull(), lit('0')).otherwise(train.clicks))

    train = train.join(carts_count,on=["user_id","merchant_id"], how="left")
    train = train.withColumn("carts", when(train.carts.isNull(), lit('0')).otherwise(train.carts))

    train = train.join(purchase_count,on=["user_id","merchant_id"], how="left")
    train = train.withColumn("purchase", when(train.purchase.isNull(), lit('0')).otherwise(train.purchase))

    train = train.join(favorite_count,on=["user_id","merchant_id"], how="left")
    train = train.withColumn("favorite", when(train.favorite.isNull(), lit('0')).otherwise(train.favorite))


    train.repartition(1).write.csv("train_add_action_type", encoding="utf-8", header=True)

    # 第五阶段完成 得到 trainset


    spark.stop()