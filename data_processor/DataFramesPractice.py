from pyspark.sql  import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def createSparkSession():
    return SparkSession.builder.appName("Introduction to DataFrame").master("local").getOrCreate()


def simpleDataFrame(session:SparkSession):
    cars_file_path = 'C:/Spark_learning/PySparkPractise/PySparkDemo/data/cars.json'
    
    # carsDF.select(col('Name')).show()
    # diectly using the sql query
    ## infer the schema and create view table using (sql table) using the createOrReplaceTempView
    carSchema = StructType([
        StructField("Name", StringType()),
        StructField("Miles_per_Gallon", DoubleType()),
        StructField("Cylinders", LongType()),
        StructField("Displacement", DoubleType()),
        StructField("Horsepower", LongType()),
        StructField("Weight_in_lbs", LongType()),
        StructField("Acceleration", DoubleType()),
        StructField("Year", StringType()),
        StructField("Origin", StringType())
    ])
    carsDF = session.read.format("json").schema(carSchema).load(cars_file_path)
    
    # Register DataFrame as a temporary view
    carsDF.createOrReplaceTempView("cars")
    
    # Example 1: Select all cars from USA
    usa_cars = session.sql("SELECT Name, Origin FROM cars WHERE Origin = 'USA'")
    usa_cars.show()
    
    session.sql("SELECT DISTINCT Cylinders FROM cars").show()

    # Example 2: Find cars with more than 150 horsepower
    high_horsepower_cars = session.sql("SELECT Name, Horsepower FROM cars WHERE Horsepower > 150")
    high_horsepower_cars.show()
    
    # Example 3: Count cars by origin
    count_by_origin = session.sql("SELECT Origin, COUNT(*) as count FROM cars GROUP BY Origin")
    count_by_origin.show()


def main():
    spark = createSparkSession()
    simpleDataFrame(spark)


if __name__ == "__main__":
    main()