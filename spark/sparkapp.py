from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("PySpark Application Started ...")

    spark = SparkSession\
            .builder\
            .appName("First PySpark Application")\
            .master("local[*]")\
            .getOrCreate()
    print(spark.sparkContext.appName)

    nameList = ["Hello", "Nepal", "Good", "Morning"]

    numList = [1,2,3,4,5]

    nameRDD = spark.sparkContext.parallelize(nameList)

    numsRDD = spark.sparkContext.parallelize(numList)

    print(nameRDD.collect())

    print(numsRDD.collect())

    spark.stop()

    print("PySpark Application Completed.")