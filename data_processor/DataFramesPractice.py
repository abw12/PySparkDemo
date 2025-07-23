from pyspark.sql  import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

def createSparkSession():
    return SparkSession.builder.appName("Introduction to DataFrame").master("local").getOrCreate()


def simpleDataFrame(session:SparkSession):
    cars_file_path = 'C:\Spark_learning\PySparkPractise\PySparkDemo\data\cars.json'
    carsDF = session.read.json(cars_file_path)
    # carsDF.select(col('Name')).show()
    # diectly using the sql query
    ## infer the schema and create view table using (sql table) using the createOrReplaceTempView
    # carSchema = StructType([
    #     StructField("Name", StringType),
    #     StructField("Miles_per_Gallon", DoubleType),
    #     StructField("Cylinders", LongType),
    #     StructField("Displacement", DoubleType),
    #     StructField("Horsepower", LongType),
    #     StructField("Weight_in_lbs", LongType),
    #     StructField("Acceleration", DoubleType),
    #     StructField("Year", StringType),
    #     StructField("Origin", StringType)
    # ])
    carsDF.createOrReplaceTempView("cars")
    powerful_engine_list = session.sql("SELECT * FROM cars WHERE Horsepower > 350").collect()
    for car in powerful_engine_list:
        print(car)


def main():
    spark = createSparkSession()
    simpleDataFrame(spark)


if __name__ == "__main__":
    main()