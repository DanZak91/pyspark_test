from pyspark.sql import SparkSession

# Создание SparkSession
spark = SparkSession.builder.appName("SQLAPIExample").getOrCreate()

# Чтение данных из JSON файлов
people_df = spark.read.json("jsons\people_SQLAPI.json")
departments_df = spark.read.json("jsons\departments__SQLAPI.json")

# Регистрация DataFrame как временные таблицы
people_df.createOrReplaceTempView("people")
departments_df.createOrReplaceTempView("departments")

# Выполнение JOIN-запроса с использованием SQL
join_df = spark.sql("""
SELECT p.name, p.age, d.department_name
FROM people p
JOIN departments d
ON p.department_id = d.id
""")

# Показ результатов
join_df.show()

# Сохранение результирующего DataFrame в CSV файл
join_df.write.mode('overwrite').csv("output.csv", header=True)