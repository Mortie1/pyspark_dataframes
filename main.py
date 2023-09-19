# I am assuming that there are 3 dataframes:
# 1st represents products (columns: id, title)
# 2nd represents categories (columns: id, title)
# 3rd represents relations between products and categories (columns: product_id, category_id)

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pyspark_dataframes").getOrCreate()


data1 = [
    (1, "Хлеб"),
    (2, "Масло"),
    (3, "Колбаса"),
    (4, "Сыр"),
    (5, "Шоколадное молоко"),
    (6, "Конфеты"),
    (7, "Сметана"),
]

data2 = [
    (1, "Хлебобулочные изделия"),
    (2, "Молочные изделия"),
    (3, "Сладкое"),
]

schema12 = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), False),
    ]
)



data3 = [(1, 1), (2, 2), (4, 2), (5, 2), (5, 3), (6, 3), (7, 2)]

schema3 = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("category_id", IntegerType(), False),
    ]
)

df1 = spark.createDataFrame(data=data1, schema=schema12)
df2 = spark.createDataFrame(data=data2, schema=schema12)
df3 = spark.createDataFrame(data=data3, schema=schema3)

df_res = df1.join(df3, df1.id == df3.product_id, "left")
df_res = df_res.join(df2.withColumnRenamed("title", "Category"), df_res.category_id == df2.id, "left")
cols_to_delete = ("id", "product_id", "category_id")
df_res = df_res.drop(*cols_to_delete).withColumnRenamed("title", "Product")
df_res.show()
