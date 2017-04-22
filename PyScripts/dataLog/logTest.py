#!/usr/bin/env python

import numpy as np
from pyspark import SparkContext, SparkConf
from operator import add

tempDist = 1.0
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

def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist = np.sum((p - centers[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex

def printStuff(line):
    print(line)
    return 0

def checkIfSame(data1,data2):
    if data1==data2:
        return 1
    else:
        return 0
    
#crime type: ndx 1, address: index 5

sc = SparkContext(appName="logReg")

lines = sc.textFile("hdfs://master:54310/user/hduser/dataLogistic/Car.csv")
data = lines.map(parseVector).cache()
data2 = data.map(lambda (k,v) : (k, v[1])).cache() 
print(str(data2.collect()))
data3 = data2.reduceByKey(add)
data4 = data3.takeOrdered(1, key=lambda x: -x[1])
data5=data.filter(lambda x:checkIfSame(x[0],data4[0][0])).cache().collect()
print(data5[0])

#print(str(data5.sample(1,5).collect()))
'''data = lines.filter(lambda x: ("BURGLARY" in x or "THEFT" in x)).map(parseVector)

data2 = data.reduceByKey(lambda x, y : x + y).cache()
#for Testing below
print(range (0, 100)+range(350, 450)+range(700,800)+range(1050, 1150))
data2 = data3.filter(lambda x: (x[1][0] in range (0, 100)+range(350, 450)+range(700,800)+range(1050, 1150) and x[1][1] in range(0,100)+range(350, 450)+range(700,800)+range(1050, 1150))).cache()
K = 5
convergeDist = .001

kPointss = data2.takeSample(False, K, 1)
kPoints=[]
#below for testing
'''
#kPointss = [('a', np.array([50, 50])),('b', np.array([400,400])), ('c', np.array([750,750])), ('d', np.array([1100, 1100]))]
'''
for kPoint in kPointss:
    kPoints.append(kPoint[1])
while tempDist > convergeDist:
    print(kPoints)
    closest = data2.map(lambda (k,v) : (closestPoint(v, kPoints), (v, 1)))
    closest.cache()
    print(str(closest.collect()[0]) + " and " + str(closest.collect()[1]))
    pointStats = closest.reduceByKey(lambda p1, p2: (p1[0] + p2[0], p1[1] + p2[1]))
    pointStats.cache()
    psc = pointStats.collect()
    print(str(psc[0][0])+str(psc[0][1]))
    newPointss = pointStats.map(lambda st: (st[0], st[1][0] / st[1][1]))
    newPoints=newPointss.collect()
    print("finished an action...")
    tempDist = sum(np.sum( (kPoints[iK] - p) ** 2) for (iK, p) in newPoints)
    for (iK, p) in newPoints:
        kPoints[iK] = p

print("Final centers: " + str(kPoints))
sc.stop()


'''
#NOTE: this converts an rdd into a normal python ds
#data3 = data2.take(10)
#print(data3)
#data.map(printStuff)
#data.saveAsTextFile('hdfs://master:54310/testkmeans.txt')
#print(parseVector("hi,dude")a
