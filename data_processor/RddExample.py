from pyspark import SparkContext
from pyspark.sql import SparkSession
from traceback import print_exc
import re


class Student:

    def __init__(self,name,subject,marks):
        self.name=name
        self.subject=subject
        self.marks=marks
    
    @staticmethod
    def fetchData():
        return (
            [
            Student("Abhishek","Science",87),
            Student("Abhishek","Math",92),
            Student("Abhishek","History",98),
            Student("Sara","Math",97),
            Student("Sara","Science",93),
            Student("Sara","History",91)
            ]
        )


def parseDataFakeFrdData(data_lines) -> tuple[int,int]:
    fields =data_lines.split(',') # 4 fields are there in the csv file
    age:int = int(fields[2]) # type caste the str to int
    numFrds:int = int(fields[3])
    return (age,numFrds)

def calcAvgFrdsData(sc:SparkContext, filePath:str):
    file_data = sc.textFile(filePath)  # this is another way to create an rdd from a file. it return rdd of str 
    rdd = file_data.map(parseDataFakeFrdData) # map the data to a specific format( rdd of (age,numFrds))
    totalsByAge = rdd.mapValues(lambda x: (x, int(1))).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])) # (age = 33, numfrds = (387,2)) - suppose for age 33 we have two entries one with (385,1) and other with (2,1) => both adds up to (387,2) here 2 represent the num of occurences(count of age 33 appeared in the entire file)
    avgByAge  = totalsByAge.mapValues(lambda x: x[0] / x[1] )
    results = avgByAge.collect() ## return a list of tuple(age, avg_frd)
    # sort the list in reverse order by key (i.e age)
    results_sorted = sorted(results, key = lambda x: x[0],reverse=True)
    for result in results_sorted:
        print(result)


def parseWeatherData(lines): 
    data=lines.split(',')
    stationId =  data[0]
    entryType = data[2]
    temperature = float(data[3]) * 0.1 * (9.0 / 5.0) + 32.0 # conversion of degree celsius to ferenite
    return (stationId,entryType,temperature)

def minTempWeatherData(sc:SparkContext, filePath):
    lines = sc.textFile(filePath)
    parsedLines = lines.map(parseWeatherData)
    minTemps = parsedLines.filter(lambda x: 'TMIN' in x[1]) # filtering the entryType with TMIN
    stationTemps = minTemps.map(lambda x: (x[0], x[2])) # form a key:value pair RDD with (key: stationId,value: temperature)
    minTemps = stationTemps.reduceByKey(lambda x,y: min(x,y)) # reduceByKey - for each unique key i.e stationId, it takes all the values (temperature) and reduces them to single value using the function you provide.
    results = minTemps.collect();

    for result in results:
         print(result[0] + "\t{:.2f}F".format(result[1])) 

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

def wordCounter(sc:SparkContext,filePath):
    lines = sc.textFile(filePath)
    words = lines.flatMap(normalizeWords) ## if you use map instead of the flatmap then you'll get list of word per line. hence using flatmap to flatten the list of word into single sequence of words  
    # wordCounts = words.countByValue() # below is the way if counting the word  hard way manually by RDD
    # for word, count in wordCounts.items():
    #     cleanword =word.encode('ascii','ignore')
    #     print(cleanword, count)

    # to count using the rdd methods and not directing use the built-in countByValue() method
    # first convert the words RDD to Key-Value Pair RDD
    wordCountPair = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y) # key-value pair = (word,count)
    ## flip the (word,count) pair to (count,word) then use sortByKey method over it
    wordCounts = wordCountPair.map(lambda pair: (pair[1],pair[0])).sortByKey()
    results = wordCounts.collect()

    for result in results:
        count =str(result[0])
        word = result[1].encode('ascii','ignore')
        if (word):
            print(f'{word}  : {count}')

def parseCutomerData(lines):
    data=lines.split(',')
    customer_id = int(data[0])
    amount = float(data[2])
    return (customer_id,amount)

def findTotalAmount(sc:SparkContext,filePath:str):
    lines=sc.textFile(filePath)
    customer_details = lines.map(parseCutomerData)
    total_spent_per_customer = customer_details.reduceByKey(lambda x,y : x + y)
    # flip the pair(tuple) and then sortByKey to get the sorted result by amount
    sorted_data = total_spent_per_customer.map(lambda pair: (pair[1],pair[0])).sortByKey()
    results = sorted_data.collect()

    for total_amount,customer_id in results:
        print(f'{customer_id} : {total_amount}')
    
    

def main():
    
    spark = SparkSession.builder.appName("RddExample").config("spark.master","local").getOrCreate() #type: ignore
    sc = spark.sparkContext
    
    # student_list = Student.fetchData()
    # # Create an RDD of student list
    # students_rdd = sc.parallelize(student_list, numSlices=3)
    # # Create (name, marks) pairs and returns an RDD
    # keyvalueRdd = students_rdd.map(lambda s: (s.name,s.marks)) ## creating a (key:value) pair rdd
    # aggregated_marks = keyvalueRdd.reduceByKey(lambda a,b: a + b) #For each unique name, sums up all the marks.
    # print(aggregated_marks.collect()) 

    # fake friends data set example of rdd key value pairs
    # calcAvgFrdsData(sc,'C:/Spark_learning/PySparkPractise/PySparkDemo/data/fakefriends.csv')

    ## weather station data using the filtering concept on the rdd
    # minTempWeatherData(sc,'C:/Spark_learning/PySparkPractise/PySparkDemo/data/1800.csv')

    ## word count by scanning the book.txt file (usage of flatMap)
    # wordCounter(sc,'C:/Spark_learning/PySparkPractise/PySparkDemo/data/book.txt')

    ## cutomer-order.csv : find the total amount spent by each customer on items/orders
    findTotalAmount(sc,'C:\Spark_learning\PySparkPractise\PySparkDemo\data\customer-orders.csv')


if __name__ == "__main__":
    main()