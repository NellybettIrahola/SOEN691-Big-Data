import os
import sys
import copy
import time
import random
import pyspark
from statistics import mean
from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import desc, size, max, abs
import numpy as np
'''
INTRODUCTION

With this assignment you will get a practical hands-on of frequent 
itemsets and clustering algorithms in Spark. Before starting, you may 
want to review the following definitions and algorithms:
* Frequent itemsets: Market-basket model, association rules, confidence, interest.
* Clustering: kmeans clustering algorithm and its Spark implementation.

DATASET

We will use the dataset at 
https://archive.ics.uci.edu/ml/datasets/Plants, extracted from the USDA 
plant dataset. This dataset lists the plants found in US and Canadian 
states.

The dataset is available in data/plants.data, in CSV format. Every line 
in this file contains a tuple where the first element is the name of a 
plant, and the remaining elements are the states in which the plant is 
found. State abbreviations are in data/stateabbr.txt for your 
information.
'''

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''

def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

def toCSVLineRDD(rdd):
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x,y: os.linesep.join([x,y]))
    return a + os.linesep

def toCSVLine(data):
    if isinstance(data, RDD):
        if data.count() > 0:
            return toCSVLineRDD(data)
        else:
            return ""
    elif isinstance(data, DataFrame):
        if data.count() > 0:
            return toCSVLineRDD(data.rdd)
        else:
            return ""
    return None


'''
PART 1: FREQUENT ITEMSETS

Here we will seek to identify association rules between states to 
associate them based on the plants that they contain. For instance, 
"[A, B] => C" will mean that "plants found in states A and B are likely 
to be found in state C". We adopt a market-basket model where the 
baskets are the plants and the items are the states. This example 
intentionally uses the market-basket model outside of its traditional 
scope to show how frequent itemset mining can be used in a variety of 
contexts.
'''


def data_frame(filename, n):
    '''
    Write a function that returns a CSV string representing the first 
    <n> rows of a DataFrame with the following columns,
    ordered by increasing values of <id>:
    1. <id>: the id of the basket in the data file, i.e., its line number - 1 (ids start at 0).
    2. <plant>: the name of the plant associated to basket.
    3. <items>: the items (states) in the basket, ordered as in the data file.

    Return value: a CSV string. Using function toCSVLine on the right 
                  DataFrame should return the correct answer.
    Test file: tests/test_data_frame.py
    '''
    spark = init_spark()
    result = spark.sparkContext.textFile(filename).map(lambda l: l.split(",")).zipWithIndex().map(lambda x: (x[1], x[0][0], x[0][1:])).take(n)
    df = spark.createDataFrame(result, ['id', 'plant','items'])

    return toCSVLine(df)

def frequent_itemsets(filename, n, s, c):
    '''
    Using the FP-Growth algorithm from the ML library (see 
    http://spark.apache.org/docs/latest/ml-frequent-pattern-mining.html), 
    write a function that returns the first <n> frequent itemsets 
    obtained using min support <s> and min confidence <c> (parameters 
    of the FP-Growth model), sorted by (1) descending itemset size, and 
    (2) descending frequency. The FP-Growth model should be applied to 
    the DataFrame computed in the previous task. 
    
    Return value: a CSV string. As before, using toCSVLine may help.
    Test: tests/test_frequent_items.py
    '''
    spark = init_spark()
    result = spark.sparkContext.textFile(filename).map(lambda l: l.split(",")).zipWithIndex().map(lambda x: (x[1], x[0][0], x[0][1:]))
    df = spark.createDataFrame(result, ['id', 'plant','items'])

    fpGrowth = FPGrowth(itemsCol="items", minSupport=s, minConfidence=c)
    model = fpGrowth.fit(df)
    result=model.freqItemsets


    result=result.select("items","freq",size("items").alias("tam"))
    result=result.sort(desc('tam'),desc('freq')).limit(n)
    result=result.select('items','freq')

    return toCSVLine(result)


def association_rules(filename, n, s, c):
    '''
    Using the same FP-Growth algorithm, write a script that returns the 
    first <n> association rules obtained using min support <s> and min 
    confidence <c> (parameters of the FP-Growth model), sorted by (1) 
    descending antecedent size in association rule, and (2) descending 
    confidence.

    Return value: a CSV string.
    Test: tests/test_association_rules.py
    '''
    spark = init_spark()
    result = spark.sparkContext.textFile(filename).map(lambda l: l.split(",")).zipWithIndex().map(lambda x: (x[1], x[0][0], x[0][1:]))
    df = spark.createDataFrame(result, ['id', 'plant', 'items'])

    fpGrowth = FPGrowth(itemsCol="items", minSupport=s, minConfidence=c)
    model = fpGrowth.fit(df)
    result = model.associationRules

    result = result.select(size("antecedent").alias('tam'),'antecedent','consequent', 'confidence')
    result = result.sort(desc('tam'), desc('confidence')).limit(n)
    result=result.select('antecedent','consequent','confidence')

    return toCSVLine(result)


def interests(filename, n, s, c):
    '''
    Using the same FP-Growth algorithm, write a script that computes 
    the interest of association rules (interest = |confidence - 
    frequency(consequent)|; note the absolute value)  obtained using 
    min support <s> and min confidence <c> (parameters of the FP-Growth 
    model), and prints the first <n> rules sorted by (1) descending 
    antecedent size in association rule, and (2) descending interest.

    Return value: a CSV string.
    Test: tests/test_interests.py
    '''
    spark = init_spark()
    result = spark.sparkContext.textFile(filename).map(lambda l: l.split(",")).zipWithIndex().map(
        lambda x: (x[1], x[0][0], x[0][1:]))
    df = spark.createDataFrame(result, ['id', 'plant', 'items'])

    fpGrowth = FPGrowth(itemsCol="items", minSupport=s, minConfidence=c)
    model = fpGrowth.fit(df)
    result = model.associationRules
    modelResult = model.freqItemsets
    result=modelResult.join(result,modelResult['items']==result["consequent"])
    total = df.count()

    result = result.withColumn("interest",abs(result["confidence"]-result["freq"]/total))
    result = result.select(size("antecedent").alias('tam'), 'antecedent', 'consequent', 'confidence',"items","freq","interest")
    result = result.sort(desc('tam'), desc('interest')).limit(n)
    result=result.select('antecedent', 'consequent', 'confidence',"items","freq","interest")

    return toCSVLine(result)


'''
PART 2: CLUSTERING

We will now cluster the states based on the plants that they contain.
We will reimplemented and use the kmeans algorithm. States will be 
represented by a vector of binary components (0/1) of dimension D, 
where D is the number of plants in the data file. Coordinate i in a 
state vector will be 1 if and only if the ith plant in the dataset was 
found in the state (plants are ordered alphabetically, as in the 
dataset). For simplicity, we will initialize the kmeans algorithm 
randomly.

An example of clustering result can be visualized in states.png in this 
repository. This image was obtained with R's 'maps' package (Canadian 
provinces, Alaska and Hawaii couldn't be represented and a different 
seed than used in the tests was used). The classes seem to make sense 
from a geographical point of view!
'''

def data_preparation(filename, plant, state):
    '''
    This function creates an RDD in which every element is a tuple with 
    the state as first element and a dictionary representing a vector 
    of plant as a second element:
    (name of the state, {dictionary})

    The dictionary should contains the plant names as a key. The 
    corresponding value should be 1 if the plant occurs in the state of 
    the tuple and 0 otherwise.

    You are strongly encouraged to use the RDD created here in the 
    remainder of the assignment.

    Return value: True if the plant occurs in the state and False otherwise.
    Test: tests/test_data_preparation.py
    '''
    spark = init_spark()
    rddLines = spark.read.text(filename).rdd
    rddLines = rddLines.map(lambda l: l.value.split(",")).map(lambda x: (x[0],1 if state in x else 0)).collectAsMap()
    stateResult=(state,rddLines)

    return (True if stateResult[1][plant]==1 else False)

def distance2(filename, state1, state2):
    '''
    This function computes the squared Euclidean
    distance between two states.
    
    Return value: an integer.
    Test: tests/test_distance.py
    '''
    spark = init_spark()
    rddLines = spark.read.text(filename).rdd
    firstState = rddLines.map(lambda l: l.value.split(",")).map(lambda x: (1 if state1 in x else 0)).zipWithIndex().map(lambda x:(x[1],x[0]))
    secondState = rddLines.map(lambda l: l.value.split(",")).map(lambda x: (1 if state2 in x else 0)).zipWithIndex().map(lambda x:(x[1],x[0]))
    result=firstState.union(secondState).reduceByKey(lambda x,y:x-y).map(lambda x:(1,x[1]**2)).reduceByKey(lambda x,y:x+y)

    return result.collect()[0][1]

def init_centroids(k, seed):
    '''
    This function randomly picks <k> states from the array in answers/all_states.py (you
    may import or copy this array to your code) using the random seed passed as
    argument and Python's 'random.sample' function.

    In the remainder, the centroids of the kmeans algorithm must be
    initialized using the method implemented here, perhaps using a line
    such as: `centroids = rdd.filter(lambda x: x['name'] in
    init_states).collect()`, where 'rdd' is the RDD created in the data
    preparation task.

    Note that if your array of states has all the states, but not in the same
    order as the array in 'answers/all_states.py' you may fail the test case or
    have issues in the next questions.

    Return value: a list of <k> states.
    Test: tests/test_init_centroids.py
    '''

    all_states = ["ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl",
                  "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
                  "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm",
                  "ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd",
                  "tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al",
                  "bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk",
                  "yt", "dengl", "fraspm"]

    random.seed(seed)
    centroids=random.sample(all_states,k)

    return centroids

def first_iter(filename, k, seed):
    '''
    This function assigns each state to its 'closest' class, where 'closest'
    means 'the class with the centroid closest to the tested state
    according to the distance defined in the distance function task'. Centroids
    must be initialized as in the previous task.

    Return value: a dictionary with <k> entries:
    - The key is a centroid.
    - The value is a list of states that are the closest to the centroid. The list should be alphabetically sorted.

    Test: tests/test_first_iter.py
    '''

    all_states = ["ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl",
                  "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
                  "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm",
                  "ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd",
                  "tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al",
                  "bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk",
                  "yt", "dengl", "fraspm"]


    spark = init_spark()
    rddLines=spark.sparkContext.parallelize(all_states)
    centroids = spark.sparkContext.parallelize(init_centroids(k, seed))
    result=rddLines.cartesian(centroids).map(lambda x:(x[0],(distance2(filename,x[0],x[1]),x[1]))).reduceByKey(lambda x,y: (x if x[0]<y[0] else y))
    divi=result.map(lambda x:(x[1][1],[x[0]])).reduceByKey(lambda x,y: x+y).map(lambda x:(x[0],sorted(x[1]))).sortByKey().collectAsMap()

    return divi

def vectorState(state,filename):
    spark = init_spark()
    rddLines = spark.read.text(filename).rdd
    stateVector = rddLines.map(lambda l: l.value.split(",")).map(lambda x: (1 if state in x else 0))

    return stateVector.collect()


def closestCen(vector,listCen):
    element = (listCen[0],vector,np.linalg.norm(np.asarray(vector[1]) - np.asarray(listCen[0][1])))
    for i in listCen:
        element1=np.linalg.norm(np.asarray(vector[1])-np.asarray(i[1]))
        if(element[2]>element1):
            element=(i,vector,element1)

    return (element[0][0],[element[1]])


def newCen1(elements):
    tam=len(elements[0][1])
    result=np.zeros(tam,dtype=int)

    for i in elements:
        element=np.asarray(i[1])
        result=np.add(element,result)
    final=np.divide(result,len(elements))
    return final

def elimVec(elements):
    lista=[]
    for i in elements:
        lista=lista+[i[0]]

    return lista

def kmeans(filename, k, seed):
    '''
    This function:
    1. Initializes <k> centroids.
    2. Assigns states to these centroids as in the previous task.
    3. Updates the centroids based on the assignments in 2.
    4. Goes to step 2 if the assignments have not changed since the previous iteration.
    5. Returns the <k> classes.

    Note: You should use the list of states provided in all_states.py to ensure that the same initialization is made.

    Return value: a list of lists where each sub-list contains all states (alphabetically sorted) of one class.
                  Example: [["qc", "on"], ["az", "ca"]] has two
                  classes: the first one contains the states "qc" and
                  "on", and the second one contains the states "az"
                  and "ca".
    Test file: tests/test_kmeans.py
    '''

    all_states = ["ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl",
                  "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
                  "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm",
                  "ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd",
                  "tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al",
                  "bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk",
                  "yt", "dengl", "fraspm"]

    spark = init_spark()
    rddLines = spark.sparkContext.parallelize(all_states)
    vectorsDict = rddLines.map(lambda x: (x, vectorState(x, filename))).cache()

    # First iter
    centroids = vectorsDict.filter(lambda x: x[0] in init_centroids(k, seed))
    centroList=centroids.map(lambda x:x[1])
    centroids=centroids.collect()
    resultAssig=vectorsDict.map(lambda x: closestCen(x,centroids)).reduceByKey(lambda x,y:x+y)
    # Other Iter

    auxBool = True
    while auxBool == True:

        newCentroid = resultAssig.map(lambda x: (newCen1(x[1]))).zipWithIndex().map(lambda x: (x[1], x[0]))
        newCentroidList=newCentroid.map(lambda x:x[1])
        newCentroid=newCentroid.collect()

        resultAssigI = vectorsDict.map(lambda x: closestCen(x, newCentroid)).reduceByKey(lambda x, y: x + y)
        inter=centroList.union(newCentroidList).collect()

        unions = np.unique(inter,axis=0)
        resultAssig = resultAssigI
        centroList=newCentroidList

        if (len(unions) == k):
            auxBool = False

    resultAssigI=resultAssigI.map(lambda x: sorted(elimVec(x[1]))).collect()
    return sorted(resultAssigI)
