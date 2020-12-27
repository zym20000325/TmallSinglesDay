import numpy as np
import pandas as pd

# 选取特征值时 testset 顺序打乱 将 result 调整为原来的顺序

myResult = pd.read_csv("result.csv")
sampleResult = pd.read_csv("test_format1.csv")

sampleResult = pd.merge(sampleResult,myResult,on=["user_id","merchant_id"],how="left")

print(sampleResult.head())

sampleResult.to_csv("FinalResult.csv")