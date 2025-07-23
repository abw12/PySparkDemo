from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField,StringType,DoubleType
import csv

class StockValue:
    def __init__(self,symbol,date,price):
        self.symbol=symbol
        self.date=date
        self.price=price
    
    def getSchema(self) -> StructType:
        return StructType([
            StructField("symbol",StringType(),True),
            StructField("date",StringType(),True),
            StructField("price",DoubleType(),True)
        ])
        

def create_spark_session():
    return (
        SparkSession.builder
        .appName("DataProcessor") # type: ignore
        .config("spark.master", "local")
        .getOrCreate() 
    )

def read_data(spark:SparkSession, filename):
    return spark.read.json(filename)

def transform_data(df):
    return df.select("Title", "US_Gross","Release_Date").filter("Worldwide_Gross > 50000")

def write_data(df, filename):
    df.write.mode("overwrite").json(filename)

def getCarsDataFrame(spark,filePath):
    carsDF=spark.read.json(filePath)
    carsDF.show()
    carsDF.withColumn("Weight_in_kg",col("Weight_in_lbs")/2.2) ## adding a new column using existing column from the DF
    carsDF.withColumnRenamed("Weight_in_lbs","Weight_in_pounds").show() ## renaming the existing coulmn
    # union - adding a DF into another DF with same schema
    more_cars_df=spark.read.option("inferSchema","true").json("./data/more_cars.json")
    allCarsDF=carsDF.union(more_cars_df)
    allCarsDF.where(col("Name") == "ferrari enzo")


def createNewDataFrame(spark):
    ## create a dataframe
    data = [("James", "Smith", "USA", "CA"),
        ("Michael", "Rose", "USA", "NY"),
        ("Robert", "Williams", "USA", "CA"),
        ("Maria", "Jones", "USA", "FL")]
    columns = ["firstname", "lastname", "country", "state"]
    df = spark.createDataFrame(data,columns)
    df.show()

def rddWorking(spark: SparkSession,path: str):
    ### working with RDD
    sc=spark.sparkContext
    ## creating RDD from a collection

    # rdd1=sc.parallelize([item for item in range(1,500)])
    # rdd1Collect=rdd1.collect()
    # print("Number Of partitions: "+ str(rdd1.getNumPartitions()))
    # print(rdd1Collect)

    ## Creating RDD from a file
    stockData=readCSVFile(path) ## return a list of stock data
    # Convert list of StockValue objects to list of Row objects
    rows = [Row(symbol=stock.symbol, date=stock.date, price=stock.price) for stock in stockData]
    stockRdd=sc.parallelize(rows)
    collected_stocks=stockRdd.collect()
    stocksDf=spark.createDataFrame(stockRdd)
    stocksDf.show()
    ## reading the file using the textFile method
    filteredRddData=sc.textFile(path).filter(lambda x: "GOOG" in x).collect()
    print(filteredRddData)

def readCSVFile(path):
    stock_values = []
    with open(path,'r') as file:
        reader = csv.reader(file)
        next(reader) ## skip the first row which is an header in stocks.csv file
        # stocker_values=[StockValue(row[0],row[1],float(row[2])) for row in reader]
        for row in reader:
            stock_value = StockValue(row[0], row[1], float(row[2]))
            stock_values.append(stock_value)
    return stock_values

# Convert StockValue objects to tuples
def stock_value_to_tuple(stock_value):
    return (stock_value.symbol, stock_value.date, stock_value.price)


def main():
    spark: SparkSession = create_spark_session()
    
    # df = read_data(spark, "./data/movies.json")
    # transformed_df = transform_data(df)
    # write_data(transformed_df, "./data/output_data")
    # transformed_df.show()
    # getCarsDataFrame(spark,"./data/cars.json")
    # createNewDataFrame(spark)
    filePath: str = "./data/stocks.csv"
    rddWorking(spark,filePath)

if __name__ == "__main__":
    main()

    
