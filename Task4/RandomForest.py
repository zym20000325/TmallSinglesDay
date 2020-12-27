from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import BinaryClassificationEvaluator      
from pyspark.ml.evaluation import MulticlassClassificationEvaluator  
from pyspark.ml.feature import StandardScaler
from pyspark.sql.functions import udf,when
from pyspark.sql.types import *


if __name__ == "__main__":


        spark = SparkSession\
                .builder\
                .appName("RandomForest")\
                .getOrCreate()



        # 读入训练集和测试集

        trainset = spark.read.csv('trainset.csv',inferSchema=True,header=True)
        testset = spark.read.csv('testset.csv',inferSchema=True,header=True)


        print("\n")
        print((trainset.count(),len(trainset.columns)))
        print((testset.count(),len(testset.columns)))

        # print("\n")
        # trainset.printSchema()

        # print("\n")
        # testset.printSchema()

        # trainset.describe().select('summary','age','gender','logs','items','cate','browse','clicks','carts','purchase','favorite').show() 
        # print("\n")
        # trainset.groupBy('label').count().show()         
        # print("\n")                                              
        # trainset.groupBy('label').mean().show()                                                        



        # 选取用于训练的特征
                                
        # data_assembler = VectorAssembler(inputCols=['age','gender','logs','items','cate','browse','clicks','carts','purchase','favorite'], outputCol="features")
        data_assembler = VectorAssembler(inputCols=['logs','items','cate','purchase'], outputCol="features")
        # data_assembler = VectorAssembler(inputCols=['logs','items','cate','browse','clicks','purchase','favorite'], outputCol="features")


        trainset = data_assembler.transform(trainset)
        testset = data_assembler.transform(testset)


        # print("\n")
        # data.printSchema()
        # print("\n")
        # data.select(['features','labels']).show(10,False)


        # 划分训练集和验证集

        train_data = trainset.select(['features','label'])        

        train,valid = train_data.randomSplit([0.8,0.2])  

        test = testset.select(['features'])                                    

        # train.groupBy('labels').count().show()
        # valid.groupBy('labels').count().show()


        # 训练模型

        rf = RandomForestClassifier(labelCol='label',numTrees=250).fit(train)    # 随机森林

        # rf = LogisticRegression(labelCol='label').fit(train)  # 逻辑回归

        # rf = NaiveBayes(labelCol='label').fit(train)  # 贝叶斯


        # 对验证集和测试集进行预测

        valid_prediction = rf.transform(valid)
        test_prediction = rf.transform(test)

        # print("\n")
        # valid_prediction.select(['probability','label','prediction']).show(10,False)
        # test_prediction.select(['probability','prediction']).show(10,False)


        # 随机森林每个属性的重要性 

        print("\n")
        print("importance of features")
        print(rf.featureImportances)

        
        # 验证集的 accuracy
       
        
        valid_accuracy = MulticlassClassificationEvaluator(labelCol='label',metricName='accuracy').evaluate(valid_prediction)
        print("\n")
        print("accuracy")
        print(format(valid_accuracy))

        # result = test_prediction.select("probability")
        # result.toPandas().to_csv("result.csv", index=False)

        # test_accuracy = MulticlassClassificationEvaluator(labelCol='labels',metricName='accuracy').evaluate(test_prediction)
        # print("\n")
        # print(format(test_accuracy))


        # 记录测试集得到的 probablity

        # prob = udf(lambda x:float(x[1]),FloatType())
        # test_prediction = test_prediction.withColumn("prob", prob("probability"))
        # result = test_prediction.select("prob")
        # result.toPandas().to_csv("result.csv", index=False)


        spark.stop()