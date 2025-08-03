from doctest import FAIL_FAST
from math import inf
from turtle import mode
from unittest.result import failfast
from pkg_resources import working_set
from pyspark.sql  import SparkSession
from pyspark.sql import DataFrame,Row
from pyspark.sql import functions as func
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

def mapper(lines):
    data = lines.split(',')
    return Row(ID=int(data[0]), name=str(data[1].encode("utf-8")), age=int(data[2]), numFrds = int(data[3]))

def fakeFriends(session:SparkSession):
    friends_file_path = 'C:/Spark_learning/PySparkPractise/PySparkDemo/data/fakefriends.csv'

    # lines = session.sparkContext.textFile(friends_file_path);   
    # people = lines.map(mapper)

    # ## infer the schema and register the DataFrame as a temporary view(table)
    # schemaPeople = session.createDataFrame(people)
    # schemaPeople.createOrReplaceTempView("people")

    # # sql can run on the dataframe that have been registered as table (temp view)
    # teenagers = session.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

    # ## the result of sql query is RDD and support all the normal RDD operations
    # for teen in teenagers.collect():
    #     print(teen)
    
    # #we can also use functions to manipulate the DataFrame
    # schemaPeople.groupBy("age").count().orderBy("age").show()

    # # Example 1: Count total number of friends for each person
    # friends_count = schemaPeople.groupBy("name").sum("numFrds")
    # friends_count.show()
    
    # # Example 2: Find the person with the maximum number of friends
    # max_friends = schemaPeople.orderBy(col("numFrds").desc()).limit(1)
    # max_friends.show()

    ## read to the DataFrame directly from the csv file
    # friendsDF = session.read.option("header","true").option("inferSchema","true").csv(friends_file_path) 
    # you can pass option by chaining the option method multiple time or you can pass a map/dict of option fileds like below
    friendsDF = session.read.options(mode="FAILFAST", header=True, inferSchema=True).csv(friends_file_path)

    # select specific columns and rename them
    # friendsDF.select(col("name").alias("username"), col("age").alias("user_age")).show()

    # Example 1: Count total number of friends for each person
    friendsDF.groupBy("name").agg(func.sum("numOfFriends").alias("total_friends")).orderBy("total_friends", ascending=False).show()

    frdsByAgeDF = friendsDF.select("age","numOfFriends")
    ## find avg frds per age group 
    # frdsByAgeDF.groupBy("age").avg("numOfFriends").show()

    ## sort in descending order
    # frdsByAgeDF.groupBy("age").avg("numOfFriends").sort("age", ascending=False).show()

    ## formatted more nicely with round-off 2 decimal
    frdsByAgeDF.groupBy("age").agg(func.round(func.avg("numOfFriends"), 2).alias("friends_avg")).sort("age", ascending=False).show()


def wordCounter(session:SparkSession):
    bookDF = session.read.text('C:/Spark_learning/PySparkPractise/PySparkDemo/data/book.txt')
    words = bookDF.select(func.explode(func.split(bookDF.value,"\\W+")).alias("word"))
    # filter out the empty words
    wordWithoutEmptyString = words.filter(col("word") != "")
    ## below line is same as writing with col method
    # wordWithoutEmptyString = words.filter(words.word != "")
    
    # normalize everything to lowercase
    lowercaseword = wordWithoutEmptyString.select(func.lower(col("word")).alias("word"))

    #count up each occurences word
    wordCount = lowercaseword.groupBy("word").count()
    # sort by count
    wordCountSorted = wordCount.sort("count")
    # show all rows/result 
    wordCountSorted.show(wordCountSorted.count())

def main():
    spark = createSparkSession()
    # simpleDataFrame(spark)
    # fakeFriends(spark)
    wordCounter(spark)
    spark.stop()



if __name__ == "__main__":
    main()