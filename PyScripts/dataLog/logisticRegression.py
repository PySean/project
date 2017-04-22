#censed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
A logistic regression implementation that uses NumPy (http://www.numpy.org)
to act on batches of input data using efficient matrix operations.
In practice, one may prefer to use the LogisticRegression algorithm in
MLlib, as shown in examples/src/main/python/mllib/logistic_regression.py.
"""
from __future__ import print_function

import sys

import numpy as np
from pyspark import SparkContext


from operator import add 
D = 56  # Number of dimensions


# Read a batch of points from the input file into a NumPy matrix object. We operate on batches to
# make further computations faster.
# The data file contains lines of the form <label> <x1> <x2> ... <xD>. We load each block of these
# into a NumPy array of size numLines * (D + 1) and pull out column 0 vs the others in gradient().
def readPointBatch(iterator):
    strs = list(iterator)
    matrix = np.zeros((len(strs), D + 1))
    for i, s in enumerate(strs):
        '''if i == 0:
            matrix = s
        else:
            r = np.r_[matrix, s]
            matrix = r'''
        matrix[i]=s
        #matrix[i] = np.fromstring(s.replace(',', ' '), dtype=np.float32, sep=' ')
        #matrix[i,:] = s
    return [matrix]

def parseVector(line):
    theSplits = line.split(',')                                             
    theSplits2 = []                                               
    for i in theSplits:  
        try:                                               
            float(i)                    
            theSplits2.append(i)      
        except:
            pass      
    return ((len(theSplits2), (theSplits2, 1)))   

def checkIfSame(data1,data2):                                                  
    if data1==data2:                                                          
        return 1 
    else:
        return 0

if __name__== "__main__":
    sc = SparkContext(appName="PythonLR")
    lines = sc.textFile("hdfs://master:54310/user/hduser/dataLogistic/Car.csv")
    data = lines.map(parseVector).cache()                
    data2 = data.map(lambda (k,v) : (k, v[1])).cache()                         
    #print("DATA2: "+str(data2.collect())) 
    data3 = data2.reduceByKey(add) 
    data4 = data3.takeOrdered(1, key=lambda x: -x[1])
    data5=data.filter(lambda x:checkIfSame(x[0],data4[0][0])).cache()
    #print("data5="+str(data5.collect()[0][1][0]))
    points2 = data5.map(lambda x:np.array(x[1][0],dtype=np.float32))
    points = points2.mapPartitions(readPointBatch).cache()
    #print("POINTS: "+str(points.collect()))
    #points = points2.mapPartitions(readPointBatch).cache()

    
    #print("POINTS: "+str(points.collect()[1]))
    #seqOp = (lambda x,y:np.vstack((x,y)))
    #combOp = (lambda x,y:np.concatenate(x,y))
    
    
    #points.aggregate(np.array([], dtype=np.int32).reshape(0,D+1), combOp, combOp)
    print("POINTS: "+str(list(points.collect())))
    iterations = int(5)
    #D = int(data5.collect()[0][0])
    # Initialize w to a random value
    w = 2 * np.random.ranf(size=D) - 1
    print("Initial w: " + str(w))
    # Compute logistic regression gradient for a matrix of data points
    def sigmoid(myVector):
        return (1/(1+np.exp(-myVector)))
    '''def gradient(matrix, w):
        Y = matrix[:, 0]    # point labels (first column of input file)
        X = matrix[:, 1:]   # point coordinates
        # For each point (x, y), compute gradient function, then sum these up
        return sigmoid(X.dot(w))'''
    def gradient(matrix, w):
        Y = matrix[:, 0]
        X = matrix[:, 1:]
        Yhat = []
        totalRight = 0
        Yhat = np.multiply((sigmoid(X.dot(w))-Y.T), X.T).T
        #.mean(axis=0)
        #Yhat = Y
        '''if abs(Y[i]-Yhat[i]) < .5:
             Yhat[i] = 1
           else:
             Yhat[i] = 0'''
        return (Yhat)
    def results(matrix, w):
        Y = matrix[:,0]
        X = matrix[:, 1:]
        YY = abs(Y.T - sigmoid(X.dot(w))) 
        totalRight = 0
        for yy in YY:
            if yy < .5:
                totalRight += 1
        return (totalRight, Y.shape[0])
    def getYandYhat(matrix, w):
        Y = matrix[:,0]
        X = matrix[:, 1:]
        Yhat = sigmoid(X.dot(w))
        compare = []
        for i, y in enumerate(Y):
            compare.append((y,Yhat[i]))
        return compare 
    def add(x, y):
        x = x + y
        return x
    def addTup(x,y):
        x=[]
        x[0] = x[0] + y[0]
    def avg(x, y):
        try:
            float(y[1])
            float(x)
            return (x[0] + y, x[1]+1)
        except:
            return (float(x) + float(y), 1)
    for i in range(10):
        print("On iteration %i" % (i + 1))
        #print("POINTS: "+str(points.collect()))
        w -= 0.1 * (np.sum(points.map(lambda m: gradient(m,w)).reduce(lambda x, y : np.concatenate((x,y), axis = 0)),axis=0))
        #counter = points.map(lambda m:results(m,w)).count()
        avgs = points.map(lambda m:results(m,w)).reduce(add)
        #.reduce(lambda x, y : np.concatenate((x,y), axis = 0))
        print(avgs)
        print(points.map(lambda m: getYandYhat(m,w)).collect())
        #Yhat = points.map(results)
        #print(str(test.collect()))
        #.reduce(add)
        #np.set_printoptions(threshold=np.nan)
        #print("Total correct="+str(Yhat))
    print("Final w: " + str(w))
    sc.stop()
