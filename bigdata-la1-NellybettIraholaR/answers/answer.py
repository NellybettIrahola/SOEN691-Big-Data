import csv
import os
import sys
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
# Dask imports
import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader

'''
INTRODUCTION

The goal of this assignment is to implement a basic analysis of textual 
data using Apache Spark (http://spark.apache.org) and 
Dask (https://dask.org). 
'''

'''
DATASET

We will study a dataset provided by the city of Montreal that contains 
the list of trees treated against the emerald ash borer 
(https://en.wikipedia.org/wiki/Emerald_ash_borer). The dataset is 
described at 
http://donnees.ville.montreal.qc.ca/dataset/frenes-publics-proteges-injection-agrile-du-frene 
(use Google translate to translate from French to English). 

We will use the 2015 and 2016 data sets available in directory `data`.
'''

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''

#Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

#Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x,y: os.linesep.join([x,y]))
    return a + os.linesep

def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

'''
Plain PYTHON implementation

To get started smoothly and become familiar with the assignment's 
technical context (Git, GitHub, pytest, Travis), we will implement a 
few steps in plain Python.
'''

#Python answer functions
def count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees (non-header lines) in
    the data file passed as first argument.
    Test file: tests/test_count.py
    Note: The return value should be an integer
    '''

    with open(filename) as f:
        for i, l in enumerate(f):
            pass
    return i

    #raise ExceptionException("Not implemented yet")

def parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks.py
    Note: The return value should be an integer
    '''
    i=0
    with open(filename) as f:
            parkReader = csv.reader(f, delimiter=',')
            next(parkReader, None)
            for row in parkReader:
                if row[6]:
                    i=i+1
    return i
    #raise Exception("Not implemented yet")

def uniq_parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of unique parks where trees
    were treated. The list must be ordered alphabetically. Every element in the list must be printed on
    a new line.
    Test file: tests/test_uniq_parks.py
    Note: The return value should be a string with one park name per line
    '''

    uniq_park = set()
    with open(filename, "r") as f:
        parkReader = csv.reader(f, delimiter=',')
        next(parkReader,None)
        for row in parkReader:
            if row[6]:
                uniq_park.add(row[6])

    return "\n".join(sorted(uniq_park))+"\n"
    # raise Exception("Not implemented yet")

def uniq_parks_counts(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that counts the number of trees treated in each park
    and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''
    strg=""
    uniq_park = dict()
    with open(filename, "r") as f:
        parkReader = csv.reader(f, delimiter=',')
        next(parkReader, None)
        for row in parkReader:
            if row[6]:
                if row[6] not in uniq_park:
                    uniq_park[row[6]]=0
                uniq_park[row[6]]=uniq_park[row[6]]+1

    sorteDic=sorted(uniq_park)
    for i in sorteDic:
        strg=strg+i+","+str(uniq_park[i])+"\n"

    return strg
    #raise Exception("Not implemented yet")

def frequent_parks_count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of the 10 parks with the
    highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar number.
    Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    strg =""
    i=0
    uniq_park = dict()
    with open(filename, "r") as f:
        parkReader = csv.reader(f, delimiter=',')
        next(parkReader, None)
        for row in parkReader:
            if row[6]:
                if row[6] not in uniq_park:
                    uniq_park[row[6]] = 0
                uniq_park[row[6]] = uniq_park[row[6]] + 1

    sorteDic = sorted(uniq_park.items(), key=lambda values:(-values[1],values[0]))

    while i<10:
        strg=strg+sorteDic[i][0]+","+str(sorteDic[i][1])+"\n"
        i=i+1

    return strg
    #raise Exception("Not implemented yet")

def intersection(filename1, filename2):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the alphabetically sorted list of
    parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''
    dataSet15=set()
    dataSet16=set()
    strg=""
    with open(filename1, "r") as f:
        parkReader = csv.reader(f, delimiter=',')
        next(parkReader, None)
        for row in parkReader:
            if row[6]:
                dataSet16.add(row[6])

    with open(filename2, "r") as f:
        parkReader = csv.reader(f, delimiter=',')
        next(parkReader, None)
        for row in parkReader:
            if row[6]:
                dataSet15.add(row[6])

    intersec=dataSet16.intersection(dataSet15)

    for x in (sorted(intersec)):
        strg=strg+x+"\n"

    return strg
    #raise Exception("Not implemented yet")
'''
SPARK RDD IMPLEMENTATION

You will now have to re-implement all the functions above using Apache 
Spark's Resilient Distributed Datasets API (RDD, see documentation at 
https://spark.apache.org/docs/latest/rdd-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
However, all operations must be re-implemented using the RDD API, you 
are not allowed to simply convert results obtained with plain Python to 
RDDs (this will be checked). Note that the function *toCSVLine* in the 
HELPER section at the top of this file converts RDDs into CSV strings.
'''

# RDD functions

def count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    sc = spark.sparkContext
    rddLines=sc.textFile(filename).map(lambda l: 1).reduce(lambda a, b: a + b)
    print(rddLines)
    return rddLines-1
    #raise Exception("Not implemented yet")
a = count_rdd("./data/frenepublicinjection2016.csv")
def parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    sc = spark.sparkContext
    rddLines = sc.textFile(filename)
    func=rddLines.first()
    data=rddLines.filter(lambda lineRow: lineRow != func).map(lambda l: next(csv.reader([l]))).map(lambda f: f[6]).filter(lambda x:x!="").count()

    return data
    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")

def uniq_parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of unique parks where
    trees were treated. The list must be ordered alphabetically. Every element
    in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_rdd.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    sc = spark.sparkContext
    rddLines = sc.textFile(filename)
    func = rddLines.first()
    data = rddLines.filter(lambda lineRow: lineRow != func).map(lambda l: next(csv.reader([l]))).map(lambda f: (f[6],"")).filter(lambda x:x[0]!="").distinct().sortByKey().keys()

    return ("\n".join(data.collect())+"\n")

    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")

def uniq_parks_counts_rdd(filename):
    '''
    Write a Python script using RDDs that counts the number of trees treated in
    each park and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts_rdd.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    sc = spark.sparkContext
    rddLines = sc.textFile(filename)
    func = rddLines.first()
    data = rddLines.filter(lambda lineRow: lineRow != func).map(lambda l: next(csv.reader([l]))).map(lambda f: (f[6], 1)).filter(lambda x: x[0] != "").reduceByKey(lambda a,b:a+b).sortByKey()

    return toCSVLine(data)

    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")

def frequent_parks_count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of the 10 parks with
    the highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar
    number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()
    sc = spark.sparkContext
    rddLines = sc.textFile(filename)
    func = rddLines.first()
    data = rddLines.filter(lambda lineRow: lineRow != func).map(lambda l: next(csv.reader([l]))).map(lambda f: (f[6], 1)).filter(lambda x: x[0] != "").\
        reduceByKey(lambda a, b: a + b).sortByKey().sortBy(lambda x:x[1],False).take(10)

    return toCSVLine(sc.parallelize(data))

    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")

def intersection_rdd(filename1, filename2):
    '''
    Write a Python script using RDDs that prints the alphabetically sorted list
    of parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()
    sc = spark.sparkContext
    rddLinesF = sc.textFile(filename1)
    rddLinesS = sc.textFile(filename2)
    funcF = rddLinesF.first()
    funcS = rddLinesS.first()
    dataF = rddLinesF.filter(lambda lineRow: lineRow != funcF).map(lambda l: next(csv.reader([l]))).map(lambda f: f[6]).filter(lambda x: x != "").distinct().sortBy(lambda x:x)
    dataS = rddLinesS.filter(lambda lineRow: lineRow != funcS).map(lambda l: next(csv.reader([l]))).map(lambda f: f[6]).filter(lambda x: x != "").distinct().sortBy(lambda x:x)
    data=dataF.intersection(dataS)

    return '\n'.join(data.collect())+"\n"
    #raise Exception("Not implemented yet")

'''
SPARK DATAFRAME IMPLEMENTATION

You will now re-implement all the tasks above using Apache Spark's 
DataFrame API (see documentation at 
https://spark.apache.org/docs/latest/sql-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
Note: all operations must be re-implemented using the DataFrame API, 
you are not allowed to simply convert results obtained with the RDD API 
to Data Frames. Note that the function *toCSVLine* in the HELPER 
section at the top of this file also converts DataFrames into CSV 
strings.
'''

# DataFrame functions

def count_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    data=spark.read.csv(filename, header=True, mode="DROPMALFORMED").count()

    return data
    #raise Exception("Not implemented yet")

def parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    data = spark.read.csv(filename, header=True, mode="DROPMALFORMED").select('Nom_parc')
    data= data.where(data.Nom_parc.isNotNull()).count()

    return data
    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")

def uniq_parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_df.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    data = spark.read.csv(filename, header=True, mode="DROPMALFORMED").select('Nom_parc')
    data = data.where(data.Nom_parc.isNotNull()).distinct().orderBy('Nom_parc')

    return toCSVLine(data)
    #raise Exception("Not implemented yet")

def uniq_parks_counts_df(filename):
    '''
    Write a Python script using DataFrames that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_df.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    data = spark.read.csv(filename, header=True, mode="DROPMALFORMED").select('Nom_parc')
    data = data.where(data.Nom_parc.isNotNull()).groupBy('Nom_parc').count().orderBy('Nom_parc')

    return toCSVLine(data)
    #raise Exception("Not implemented yet")

def frequent_parks_count_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''
    spark = init_spark()
    data = spark.read.csv(filename, header=True, mode="DROPMALFORMED").select('Nom_parc')
    data = data.where(data.Nom_parc.isNotNull()).groupBy('Nom_parc').count().orderBy('Nom_parc').orderBy('count',ascending=False).limit(10)

    return toCSVLine(data)
    #raise Exception("Not implemented yet")

def intersection_df(filename1, filename2):
    '''
    Write a Python script using DataFrames that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()
    data1 = spark.read.csv(filename1, header=True, mode="DROPMALFORMED").select('Nom_parc')
    data1 = data1.where(data1.Nom_parc.isNotNull()).distinct().orderBy('Nom_parc')
    data1=data1.select(data1.Nom_parc.alias('park_name'))
    data2 = spark.read.csv(filename2, header=True, mode="DROPMALFORMED").select('Nom_parc')
    data2 = data2.where(data2.Nom_parc.isNotNull()).distinct().orderBy('Nom_parc')
    data=data1.join(data2, data2.Nom_parc == data1.park_name).select('park_name')

    return toCSVLine(data)
    #raise Exception("Not implemented yet")

'''
DASK IMPLEMENTATION (bonus)

You will now re-implement all the tasks above using Dask (see 
documentation at http://docs.dask.org/en/latest). Outputs must be 
identical to the ones obtained previously. Note: all operations must be 
re-implemented using Dask, you are not allowed to simply convert 
results obtained with the other APIs.
'''

# Dask functions

def count_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_dask.py
    Note: The return value should be an integer
    '''
    dataFrame = df.read_csv(filename,dtype='str')# ADD YOUR CODE HERE
    return len(dataFrame)
    #raise Exception("Not implemented yet")

def parks_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_dask.py
    Note: The return value should be an integer
    '''
    dataFrame = df.read_csv(filename, dtype='str')
    dataFrame=dataFrame[dataFrame.Nom_parc.notnull()]
    return len(dataFrame)
    #raise Exception("Not implemented yet")

def uniq_parks_dask(filename):
    '''
    Write a Python script using Dask that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_dask.py
    Note: The return value should be a CSV string
    '''
    dataFrame = df.read_csv(filename, dtype='str')
    dataFrame = dataFrame[dataFrame.Nom_parc.notnull()]
    dataFrame=dataFrame.groupby('Nom_parc').count()

    return '\n'.join(dataFrame.compute().index)+'\n'
    #raise Exception("Not implemented yet")

def uniq_parks_counts_dask(filename):
    '''
    Write a Python script using Dask that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_dask.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''
    dataFrame = df.read_csv(filename, dtype='str')
    dataFrame = dataFrame[dataFrame.Nom_parc.notnull()]
    dataFrame = dataFrame.groupby('Nom_parc').count()
    dataFrame=dataFrame.Nom_arrond.compute()
    listValuesNames=(lambda x,y:x+","+y)
    listElements=listValuesNames(dataFrame.index,dataFrame.values.astype(str))

    return "\n".join(listElements)+"\n"
    #raise Exception("Not implemented yet")

def frequent_parks_count_dask(filename):
    '''
    Write a Python script using Dask that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''
    dataFrame = df.read_csv(filename, dtype='str')
    dataFrame = dataFrame[dataFrame.Nom_parc.notnull()]
    dataFrame = dataFrame.groupby('Nom_parc').count()
    dataFrame = dataFrame.reset_index().set_index('Nom_arrond')
    dataFrame = dataFrame.map_partitions(lambda x: x.sort_values(['latitude', 'Nom_parc'], ascending=[False, True]))
    dataFrame = dataFrame.reset_index().set_index('Nom_parc', sorted=True)
    computeData = dataFrame.Nom_arrond.head(10)
    listValuesNames = (lambda x, y: x + "," + y)
    listElements = listValuesNames(computeData.index, computeData.values.astype(str))

    return "\n".join(listElements)+"\n"
    #raise Exception("Not implemented yet")

def intersection_dask(filename1, filename2):
    '''
    Write a Python script using Dask that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''
    dataFrame = df.read_csv(filename1, dtype='str')
    dataFrame = dataFrame[dataFrame.Nom_parc.notnull()]
    dataFrame = dataFrame.groupby('Nom_parc').count()

    dataFrame1 = df.read_csv(filename2, dtype='str')
    dataFrame1 = dataFrame1[dataFrame1.Nom_parc.notnull()]
    dataFrame1 = dataFrame1.groupby('Nom_parc').count()

    dataFinal=dataFrame.merge(dataFrame1,on='Nom_parc')

    return '\n'.join(dataFinal.compute().index)+'\n'
    #raise Exception("Not implemented yet")
