from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType # для 1 способа

# 1ый способ создания DATAFRAME c явным указанием схемы, т.е описание типов данных
spark = SparkSession.builder \
    .appName("schema") \
    .config("spark.master", "local[*]") \
    .getOrCreate()

data = [("Alice", 25), ("Artem", 78), ("Bob", 41)]
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
])

df = spark.createDataFrame(data, schema)
df.printSchema()
# df.show()



# 2ой способ создания DATAFRAME без указания явной схемы, поскольку определяет автоматом
data2 = [
    {"name":"Vasya", "age":12},
    {"name":"Goga", "age":40},
    {"name":"Lena", "age":36},
]

df2 = spark.createDataFrame(data2)
df2.printSchema()
print(df2.dtypes)

# 3 способ

data3 = [("Alice", 25), ("Artem", 78), ("Bob", 41)]
rdd3 = spark.sparkContext.parallelize(data3)  #делаем RDD
df3 = rdd3.toDF(["name", "age"]) #Метод .toDF преобразует RDD в Dataframe
df3.printSchema()
df3.show()
spark.stop()

from pyspark import SparkContext

sc = SparkContext("local", "Transformations Example")
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# Применение преобразования map
squared_rdd = rdd.map(lambda x: x * x)
print(squared_rdd.take(3))