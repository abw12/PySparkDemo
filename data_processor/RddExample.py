from pyspark import SparkContext
from pyspark.sql import SparkSession


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


def parseData(data_lines) -> tuple[int,int]:
    fields =data_lines.split(',') # 4 fields are there in the csv file
    age:int = int(fields[2]) # type caste the str to int
    numFrds:int = int(fields[3])
    return (age,numFrds)

def calcAvgFrdsData(sc:SparkContext, filePath:str):
    file_data = sc.textFile(filePath)  # this is another way to create an rdd from a file. it return rdd of str 
    rdd = file_data.map(parseData) # map the data to a specific format( rdd of (age,numFrds))
    totalsByAge = rdd.mapValues(lambda x: (x, int(1))).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])) # (age = 33, numfrds = (387,2)) - suppose for age 33 we have two entries one with (385,1) and other with (2,1) => both adds up to (387,2) here 2 represent the num of occurences(count of age 33 appeared in the entire file)
    avgByAge  = totalsByAge.mapValues(lambda x: x[0] / x[1] )
    results = avgByAge.collect()
    for result in results:
        print(result)

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

    # fake friends data set example of rdd 
    calcAvgFrdsData(sc,'C:/Spark_learning/PySparkPractise/PySparkDemo/data/fakefriends.csv')


if __name__ == "__main__":
    main()