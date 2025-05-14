from pyspark.sql import SparkSession

# Создаем объект SparkSession
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .config("spark.master", "local[2]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

data1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = sc.parallelize(data1)
rdd_mapped = rdd.map(lambda x: x * 2)   #MAP -Метод map в Apache Spark выполняет поэлементное преобразование RDD (Resilient Distributed Dataset). Что делает map в вашем примере: Принимает функцию (lambda x: x * 2), которая применяется к каждому элементу RDD. Возвращает новый RDD, где каждый исходный элемент умножен на 2.
print(f'MAP выполняет поэлементное преобразование RDD {rdd_mapped.collect()}')



data2 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = sc.parallelize(data2)
rdd_filter = rdd.filter(lambda x: x % 2==0) # Метод filter в Apache Spark используется для выбора элементов из RDD (Resilient Distributed Dataset), которые удовлетворяют заданному условию. Принимает функцию-условие (lambda x: x % 2 == 0), которая проверяет, является ли число чётным (x % 2 == 0). Возвращает новый RDD, содержащий только те элементы, для которых условие вернуло True. Отбрасывает элементы, не удовлетворяющие условию (те, для которых условие вернуло False).
print(f'\nFILTER используется для выбора элементов из RDD которые удовлетворяют заданному условию {rdd_filter.collect()}')

data3 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = sc.parallelize(data3)
rdd_flatMap = rdd.flatMap(lambda x: [x, x * 2]) # Метод flatMap распаковывает возвращённые списки и объединяет их элементы в единый плоский список.
print(f'\nFlatMAP распаковывает возвращённые списки и объединяет их элементы в единый плоский список. {rdd_flatMap.collect()}')


data4 = [1, 1, 2, 3, 3, 4, 4, 4]
rdd = sc.parallelize(data4)
rdd_mapped_distinct = rdd.distinct()                     #distinct - Удаляет дубли
print(f'\nDISTINCT удаляет дубли {rdd_mapped_distinct.collect()}')


data_first = [115, 2, 3]
rdd_first = sc.parallelize(data_first)
data_second = [3, 4, 5, 6]
rdd_second = sc.parallelize(data_second)
rdd_union = rdd_first.union(rdd_second)         #Метод .union() в Apache Spark объединяет два RDD в один, сохраняя все элементы из обоих RDD, включая дубликаты. Это аналог операции UNION ALL в SQL.
print(f'UNION объединяет эдементы обоих RDD включая ДУБЛИ {rdd_union.collect()}')
print(f'COUNT Количесвто элементов в RDD (длина списка "типа LEN") {rdd_union.count()}')  #метод COUNT - Количесвто элементов в RDD (длина списка "типа LEN

print(f'FIRST Возвращает 1ый в списке (RDD) Элемент {rdd_union.first()}')
print(f'TAKE Возвращает ПЕРВЫЕ "n" строк (элементов) {rdd_union.take(2)}')


spark.stop()