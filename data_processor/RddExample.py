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



def main():
    student_list = Student.fetchData()
    spark = SparkSession.builder.appName("RddExample").config("spark.master","local").getOrCreate() #type: ignore
    sc = spark.sparkContext
    # Create an RDD of student list
    students_rdd = sc.parallelize(student_list)
    # Create (name, marks) pairs and returns an RDD
    keyvalueRdd = students_rdd.map(lambda s: (s.name,s.marks)) ## creating a (key:value) pair rdd
    aggregated_marks = keyvalueRdd.reduceByKey(lambda a,b: a + b) #For each unique name, sums up all the marks.
    print(aggregated_marks.collect()) 


if __name__ == "__main__":
    main()