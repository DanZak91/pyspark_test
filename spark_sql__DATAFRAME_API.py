import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


findspark.init()


# Создание SparkSession
spark = SparkSession.builder.appName("DataFrameAPIExample").getOrCreate()

# Чтение данных из JSON файла
df = spark.read.json("people_data.json")

# Фильтрация данных
filtered_df = df.filter(col("age") > 30)

# Группировка и агрегация данных
grouped_df = df.groupBy("department") \
              .agg({"age": "avg", "name": "count"}) \
              .withColumnRenamed("avg(age)", "avg_age") \
              .withColumnRenamed("count(name)", "count")

# Сортировка данных
sorted_df = grouped_df.orderBy(col("count").desc())

# Показ результатов
filtered_df.show()
sorted_df.show()

# Сохранение результирующего DataFrame в CSV файл
sorted_df.write.mode('overwrite').csv("output.csv", header=True)