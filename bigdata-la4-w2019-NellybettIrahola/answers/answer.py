from pyspark.sql import SparkSession
from pretty_print_dict import pretty_print_dict as ppd
from pretty_print_bands import pretty_print_bands as ppb
import random
import math
import numpy as np
from scipy.special import lambertw
import sys
from io import StringIO
# Dask imports
import dask.bag as db
import dask.dataframe as df


all_states = ["ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc",
              "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la",
              "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv",
              "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa",
              "pr", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "vi",
              "wa", "wv", "wi", "wy", "al", "bc", "mb", "nb", "lb", "nf",
              "nt", "ns", "nu", "on", "qc", "sk", "yt", "dengl", "fraspm"]


def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


def toCSVLineRDD(rdd):
    """This function is used by toCSVLine to convert an RDD into a CSV string

    """
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x, y: "\n".join([x, y]))
    return a + "\n"


def toCSVLine(data):
    """This function convert an RDD or a DataFrame into a CSV string

    """
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

def createDic(data_file,state):
    spark = init_spark()
    rddLines = spark.read.text(data_file).rdd
    stateName=spark.sparkContext.parallelize([("name",state)])
    stateVector = rddLines.map(lambda l: l.value.split(",")).map(lambda x: (x[0], (1 if state in x else 0))).union(stateName)
    return stateVector.collectAsMap()

def data_preparation(data_file, key, state):
    """Our implementation of LSH will be based on RDDs. As in the clustering
    part of LA3, we will represent each state in the dataset as a dictionary of
    boolean values with an extra key to store the state name.
    We call this dictionary 'state dictionary'.

    Task 1 : Write a script that
             1) Creates an RDD in which every element is a state dictionary
                with the following keys and values:

                    Key     |         Value
                ---------------------------------------------
                    name    | abbreviation of the state
                    <plant> | 1 if <plant> occurs, 0 otherwise

             2) Returns the value associated with key
                <key> in the dictionary corresponding to state <state>

    *** Note: Dask may be used instead of Spark.

    Keyword arguments:
    data_file -- csv file of plant name/states tuples (e.g. ./data/plants.data)
    key -- plant name
    state -- state abbreviation (see: all_states)
    """
    spark = init_spark()
    statesAll=spark.sparkContext.parallelize(all_states).map(lambda x: createDic(data_file,x))
    result=statesAll.filter(lambda x: x["name"]==state).collect()

    return result[0][key]
#a = data_preparation("./data/plants.data", "tephrosia candida", "az")

def isPrime(element,listPre):
    spark = init_spark()
    result=spark.sparkContext.parallelize(listPre).map(lambda x: 1 if element%x==0 else 0).sum()

    return (0 if result==0 else 1)

def primes(n, c):
    """To create signatures we need hash functions (see next task). To create
    hash functions,we need prime numbers.

    Task 2: Write a script that returns the list of n consecutive prime numbers
    greater or equal to c. A simple way to test if an integer x is prime is to
    check that x is not a multiple of any integer lower or equal than sqrt(x).

    Keyword arguments:
    n -- integer representing the number of consecutive prime numbers
    c -- minimum prime number value
    """
    spark = init_spark()

    possible=list(range(c,c+math.ceil(n*math.log(c,2))))
    rddLines = spark.sparkContext.parallelize(possible).map(lambda x:(isPrime(x,list(range(2,math.floor(math.sqrt(x)+1)))),[x])).reduceByKey(lambda x,y: x+y).collectAsMap()

    return rddLines[0][0:(0+n)]
#primes(10, 41)


def hashFunction(m,p):

    a = random.randint(1, m)
    b = random.randint(1, m)

    x=(lambda x: ((a*x)+b) % p)

    return x


def hash_plants(s, m, p, x):
    """We will generate hash functions of the form h(x) = (ax+b) % p, where a
    and b are random numbers and p is a prime number.

    Task 3: Write a function that takes a pair of integers (m, p) and returns
    a hash function h(x)=(ax+b)%p where a and b are random integers chosen
    uniformly between 1 and m, using Python's random.randint. Write a script
    that:
        1. initializes the random seed from <seed>,
        2. generates a hash function h from <m> and <p>,
        3. returns the value of h(x).

    Keyword arguments:
    s -- value to initialize random seed from
    m -- maximum value of random integers
    p -- prime number
    x -- value to be hashed
    """
    random.seed(s)
    h=hashFunction(m,p)

    return h(x)
#hash_plants(123, 34781, 34781, 0)

def getList(n,m):

    listLambda = []
    for p in primes(n, m):
        listLambda = listLambda + [hashFunction(m, p)]

    return listLambda

def hash_list(s, m, n, i, x):
    """We will generate "good" hash functions using the generator in 3 and
    the prime numbers in 2.

    Task 4: Write a script that:
        1) creates a list of <n> hash functions where the ith hash function is
           obtained using the generator in 3, defining <p> as the ith prime
           number larger than <m> (<p> being obtained as in 1),
        2) prints the value of h_i(x), where h_i is the ith hash function in
           the list (starting at 0). The random seed must be initialized from
           <seed>.

    Keyword arguments:
    s -- seed to intialize random number generator
    m -- max value of hash random integers
    n -- number of hash functions to generate
    i -- index of hash function to use
    x -- value to hash
    """
    random.seed(s)
    listLambda=getList(n,m)

    return listLambda[i](x)
#hash_list(123, 150, 10, 7, 32)

def hashAux(hashFunctions,x):
    total=[]
    for i in range(0, len(hashFunctions)):
        total=total+[(i,hashFunctions[i](x[1]))]
    return total

def signatures(datafile, seed, n, state):
    """We will now compute the min-hash signature matrix of the states.

    Task 5: Write a function that takes build a signature of size n for a
            given state.

    1. Create the RDD of state dictionaries as in data_preparation.
    2. Generate `n` hash functions as done before. Use the number of line in
       datafile for the value of m.
    3. Sort the plant dictionary by key (alphabetical order) such that the
       ordering corresponds to a row index (starting at 0).
       Note: the plant dictionary, by default, contains the state name.
       Disregard this key-value pair when assigning indices to the plants.
    4. Build the signature array of size `n` where signature[i] is the minimum
       value of the i-th hash function applied to the index of every plant that
       appears in the given state.


    Apply this function to the RDD of dictionary states to create a signature
    "matrix", in fact an RDD containing state signatures represented as
    dictionaries. Write a script that returns the string output of the RDD
    element corresponding to state '' using function pretty_print_dict
    (provided in answers).

    The random seed used to generate the hash function must be initialized from
    <seed>, as previously.

    ***Note: Dask may be used instead of Spark.

    Keyword arguments:
    datafile -- the input filename
    seed -- seed to initialize random int generator
    n -- number of hash functions to generate
    state -- state abbreviation
    """

    spark = init_spark()
    rddLines = spark.read.text(datafile).rdd
    random.seed(seed)
    m = rddLines.count()
    hashFunction = getList(n, m)

    stateVector = rddLines.map(lambda l: l.value.split(",")).map(lambda x: (x[0], (1 if state in x else 0)))
    stateVector = stateVector.sortByKey().values().zipWithIndex().filter(lambda x:x[0]!=0).flatMap(lambda x: hashAux(hashFunction,x)).reduceByKey(lambda a,b:min(a,b))

    return stateVector.collectAsMap()
#result=signatures("./data/plants.data", 123, 10, "qc")


def hash_band(datafile, seed, state, n, b, n_r):
    """We will now hash the signature matrix in bands. All signature vectors,
    that is, state signatures contained in the RDD computed in the previous
    question, can be hashed independently. Here we compute the hash of a band
    of a signature vector.

    Task: Write a script that, given the signature dictionary of state <state>
    computed from <n> hash functions (as defined in the previous task),
    a particular band <b> and a number of rows <n_r>:

    1. Generate the signature dictionary for <state>.
    2. Select the sub-dictionary of the signature with indexes between
       [b*n_r, (b+1)*n_r[.
    3. Turn this sub-dictionary into a string.
    4. Hash the string using the hash built-in function of python.

    The random seed must be initialized from <seed>, as previously.

    Keyword arguments:
    datafile --  the input filename
    seed -- seed to initialize random int generator
    state -- state to filter by
    n -- number of hash functions to generate
    b -- the band index
    n_r -- the number of rows
    """

    spark = init_spark()
    rddLines = spark.read.text(datafile).rdd
    random.seed(seed)
    m = rddLines.count()
    hashFunction = getList(n, m)

    stateVector = rddLines.map(lambda l: l.value.split(",")).map(lambda x: (x[0], (1 if state in x else 0)))
    stateVector = stateVector.sortByKey().values().zipWithIndex().filter(lambda x: x[0] != 0).flatMap(
        lambda x: hashAux(hashFunction, x)).reduceByKey(lambda a, b: min(a, b)).filter(lambda x:b*n_r<=x[0]<(b+1)*n_r)

    return hash(str(stateVector.collectAsMap()))
#hash_band("./data/plants.data", 123, "qc", 12, 2, 2)

def getVector(state,datafile):
    spark = init_spark()
    rddLines = spark.read.text(datafile).rdd

    stateVector = rddLines.map(lambda l: l.value.split(",")).map(lambda x: (x[0], (1 if state in x else 0)))
    stateVector = stateVector.sortByKey().values().zipWithIndex().filter(lambda x: x[0] != 0)

    return stateVector.collect()

def signatureMatrix(vectorDict, functions):
    i=0
    dictionary=[]
    items = vectorDict

    while i<len(functions):
        value = functions[i](items[0][1])
        for x in items:
            value = min(value,functions[i](x[1]))

        dictionary=dictionary+[value]
        i=i+1
    a=list(zip(range(0,len(dictionary)),dictionary))

    return a

def hash_bands1(vector,n_b,n_r):
    spark = init_spark()
    bands=spark.sparkContext.parallelize(range(0,n_b))
    result=bands.map(lambda b: (b,hash(str(dict(vector[1][b * n_r:(b + 1) * n_r]))))).map(lambda x: (x,vector[0]))

    return result.collect()

def hash_bands(data_file, seed, n_b, n_r):
    """We will now hash the complete signature matrix

    Task: Write a script that, given an RDD of state signature dictionaries
    constructed from n=<n_b>*<n_r> hash functions (as in 5), a number of bands
    <n_b> and a number of rows <n_r>:

    1. maps each RDD element (using flatMap) to a list of ((b, hash),
       state_name) tuples where hash is the hash of the signature vector of
       state state_name in band b as defined in 6. Note: it is not a triple, it
       is a pair.
    2. groups the resulting RDD by key: states that hash to the same bucket for
       band b will appear together.
    3. returns the string output of the buckets with more than 2 elements
       using the function in pretty_print_bands.py.

    That's it, you have printed the similar items, in O(n)!

    Keyword arguments:
    datafile -- the input filename
    seed -- the seed to initialize the random int generator
    n_b -- the number of bands
    n_r -- the number of rows in a given band
    """
    spark = init_spark()
    random.seed(seed)
    rddLines = spark.read.text(data_file).rdd
    n=n_b*n_r
    m = rddLines.count()
    hashFunction = getList(n, m)
    statesAll = spark.sparkContext.parallelize(all_states).map(lambda x: (x,getVector(x,data_file))).map(lambda x:(x[0],signatureMatrix(x[1],hashFunction)))
    values=statesAll.flatMap(lambda x: hash_bands1(x,n_b,n_r)).groupByKey().map(lambda x:(x[0],sorted(list(x[1])))).filter(lambda x: len(x[1])>=2).sortByKey()

    return ppb(values)
#hash_bands("./data/plants.data", 123, 5, 7)

def get_b_and_r(n, s):
    """The script written for the previous task takes <n_b> and <n_r> as
    parameters while a similarity threshold <s> would be more useful.

    Task: Write a script that prints the number of bands <b> and rows <r> to be
    used with a number <n> of hash functions to find the similar items for a
    given similarity threshold <s>. Your script should also print <n_actual>
    and <s_actual>, the actual values of <n> and <s> that will be used, which
    may differ from <n> and <s> due to rounding issues. Printing format is
    found in tests/test-get-b-and-r.txt

    Use the following relations:

     - r=n/b
     - s=(1/b)^(1/r)

    Hint: Johann Heinrich Lambert (1728-1777) was a Swiss mathematician

    Keywords arguments:
    n -- the number of hash functions
    s -- the similarity threshold
    """

    b=1/(-lambertw(-(n*np.log(s))) / (n*np.log(s)))
    b=math.floor(b)
    r=int(n/b)

    n=r*b
    s=math.pow((1/b),(1/r))

    stringFinal="b="+str(b)+"\n"+"r="+str(r)+"\n"+"n_real="+str(n)+"\n"+"s_real="+str(s)+"\n"

    return stringFinal
#out = get_b_and_r(99, 0.8189373540396242)