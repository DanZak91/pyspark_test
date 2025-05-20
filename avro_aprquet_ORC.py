from pyspark.sql import SparkSession

# Запись данных в AVRO
# Создание SparkSession
spark = (
    SparkSession.builder
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.5")
        .appName("Write Avro Example")
        .getOrCreate()
)
# Пример данных
data = [("Alice", 25), ("Bob", 30), ("Cathy", 29)]
df = spark.createDataFrame(data, ["Name", "Age"])
# Запись данных в Avro
df.write.mode('overwrite').format("avro").save("path/to/output/avro")




# Запись данных в PARQUET
# Создание SparkSession
spark2 = (
    SparkSession.builder
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.5")
        .appName("Write Parquet Example")
        .getOrCreate()
)
# Пример данных
data2 = [("Alice2", 25), ("Bob2", 30), ("Cathy2", 29)]
df2 = spark2.createDataFrame(data2, ["Name", "Age"])
# Запись данных в Parquet
df2.write.mode('overwrite').parquet("path/to/output/parquet")



# Запись данных в ORC
spark3 = (
    SparkSession.builder
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.5")
        .appName("Write ORC Example")
        .getOrCreate()
)
#Пример данных
data3 = [("Alice3", 25), ("Bob3", 30), ("Cathy3", 29)]
df3 = spark3.createDataFrame(data3, ["Name", "Age"])
# Запись данных в ORC
df3.write.mode('overwrite').orc("path/to/output/orc")






#Чтение данных из Avro
df_avro = spark.read.format("avro").load("path/to/output/avro")
df_avro.show()

# Чтение данных из Parquet
df_parquet = spark.read.parquet("path/to/output/parquet")
df_parquet.show()

# Чтение данных из ORC
df_orc = spark3.read.orc("path/to/output/orc")
df_orc.show()

spark.stop()
spark2.stop()
spark3.stop()