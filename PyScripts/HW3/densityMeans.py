#!/usr/bin/env python

import numpy as np
import random
from pyspark import SparkContext, SparkConf

tempDist = 1.0
def parseVector(line):
    theSplits = line.split(',')
    if ('BURGLARY' in theSplits[1]):
        return (theSplits[5], np.array([1, 0]))
    #We're good with a blanket case because we already filter the other stuff out.
    elif ('THEFT' == theSplits[1]):
        return (theSplits[5], np.array([0, 1]))

    else:
        return ("nothin", np.array([0,0]))
def getPointsForTesting(line):
    theSplits = line.split(',')
    Point = []
    for dimension in theSplits:    
        Point.append(int(dimension))
    return(np.array(Point))

def getDistance(p, centers):
    distances = []
    for i in range(len(centers)):
        distances.append(np.sum(p-centers[i])**2)
    return (p,distances)

def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist = (np.sum(p - centers[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex

def printStuff(line):
    print(line)
    return 0


K = 3
#crime type: ndx 1, address: index 5
sc = SparkContext(appName="kmeans")
lines = sc.textFile("hdfs://master:54310/user/hduser/testFiles3")
data = lines.map(getPointsForTesting).cache()
numPoints = data.count()
maxClusterSize=numPoints/K+1
print("num points = " + str(numPoints) + ", maxClusterSize = " + str(maxClusterSize))
print("testPoints"+str(data))

convergeDist = .00001
kPointss = data.takeSample(False, K, random.randint(1,100))
kPoints = []
testPoint = np.array([1, 1])
distancesToCentroids=[]
for kPoint in kPointss:
    kPoints.append(kPoint)
#print("kpoints:" + str(kPoints))
while tempDist > convergeDist:
    #print(kPoints)
    #print(str(closest.collect()[0]) + " and " + str(closest.collect()[1]))
    #print("CLOSESTPOINT: "+ str(closestPoint(data.collect(), kPoints)))
    
    #makes a map of (centroidIndex, (aPointInside, 1))
    closest = data.map(lambda x : (closestPoint(x, kPoints), (x, 1))).cache()
    distancesToCentroids=data.map(lambda x: getDistance(x, kPoints))
    print("Distances to each k by point: \n"+str(distancesToCentroids.collect()))
    point = distancesToCentroids.min(key=lambda x:x[1][K-1])
    RDDClusters=distancesToCentroids.filter(lambda (p, ds): np.array_equal(point[0],p)).map(lambda x: (K-1, (x[0],1))).cache()
    distancesToCentroids = distancesToCentroids.filter(lambda (p, ds): not np.array_equal(point[0],p)).cache()
    
    while (distancesToCentroids.count() > 0):
        for k in range(K):
            if (distancesToCentroids.count() > 0):
                point = distancesToCentroids.min(key=lambda x:x[1][k])
                RDDClusters=RDDClusters.union(distancesToCentroids.filter(lambda (p, ds): np.array_equal(point[0],p)).map(lambda x: (k, (x[0],1)))).cache()
                distancesToCentroids = distancesToCentroids.filter(lambda (p, ds): not np.array_equal(point[0],p)).cache()
    print("testing that points get removed after being put into clusters:\n"+str(distancesToCentroids.collect()))
    print("testing RDD 0: \n" + str(RDDClusters.collect()))
    #print("test min: \n"+str(distancesToCentroids.min(key=lambda x:x[1][1])))
    #print("test filter: \n"+str(distancesToCentroids.filter(lambda (p, ds): not np.array_equal(testPoint,p)).collect()))
    #test closesti
    #while(distancesToCentroids.collect())
    
    #replacing old code with new clusters
    #testClosest = closest.groupByKey().collect()
    testClosest = RDDClusters.groupByKey().collect()


    print ("---------------NEW ITERATION-------------")
    for centroid in testClosest:
        print("Cluster with center "+str(kPoints[centroid[0]]) + ", " + str(list(centroid[1])))
    #print("centroids and their points:"+str(closest.groupByKey().collect()[:][0])+str(list(closest.groupByKey().collect()[:,1])))

    #reduces to (centroidIndex, (sumOfPointsInside.StillKDimensionalPoints, numberOfPointsInside))
    pointStats = RDDClusters.reduceByKey(lambda p1, p2: (p1[0] + p2[0], p1[1] + p2[1]))
    
    #testing pointStats
    pointStats.cache()
    psc = pointStats.collect()
    #print("testing pointStats:"+str(psc[0])+str(psc[1]))
    
    #mapping to (centroidIndex, (sumOfPointsInside/numberOfPointsInside).stillKDimensional)
    newPointss = pointStats.map(lambda st: (st[0], [st[1][0] / st[1][1], st[1][1]]))
    
    #testing newPointss
    newPoints=newPointss.collect()
    print("finished an action...newPoints=" + str(newPoints))
   
    #for each cluster, sum the cost distances of its old centroid to its new centrioid, this is your threshold, meaning this threshold is highly tied to scale 
    tempDist = sum(np.sum( (kPoints[Ki] - p[0]) ** 2) for (Ki, p) in newPoints) 


    #CsHECK FOR EMPTY CLUSTER AND MAKE A NEW ONE BY TAKING ANOTHER RANDOM POINT IF NEEDED
    '''missingIndex = -1
    if len(newPoints) != K:
        for centroidIndex in range(K):
            found = 0
            for newPoint in newPoints:
                if centroidIndex == newPoint[0]:
                    found = 1
            if found == 0:
                missingIndex = centroidIndex
        freshCentroid = data.takeSample(False, 1, random.randint(1,100))
        #print("REPLACING " + str(kPoints[missingIndex]) + " with " + str(freshCentroid[0]))
        kPoints[missingIndex]=freshCentroid[0]
       
        #Set temp distance to 1 to force another iteration with new point
        tempDist = 1
        
        #print("empty cluster: " + str(missingIndex))
    '''
    #test tempDist
    #print ("tempDist = " + str(tempDist))
    
    for (Ki, p) in newPoints:
        kPoints[Ki] = p[0]
        #print("new kpoints"+str(kPoints))
#closest = data.map(lambda x : (closestPoint(x, kPoints), (x, 1))).cache()
#testClosest = closest.groupByKey().collect()
#print("Cluster with center FINAL REGULAR COMPUTE:\n"+str(kPoints[centroid[0]]) + ", " + str(list(centroid[1]))) 

#pointStats = closest.reduceByKey(lambda p1, p2: (p1[0] + p2[0], p1[1] + p2[1]))
#newPointss = pointStats.map(lambda st: (st[0], [st[1][0] / st[1][1], st[1][1]]))
#newPoints=newPointss.collect()
#print("finished an action...newPoints=" + str(newPoints))


#tempDist = sum(np.sum( (kPoints[Ki] - p[0]) ** 2) for (Ki, p) in newPoints)
for (Ki, p) in newPoints:                                                   
        kPoints[Ki] = p[0]

closest = data.map(lambda x : (closestPoint(x, kPoints), (x, 1))).cache()       
testClosest = closest.groupByKey().collect()                                    
for centroid in testClosest:
    print("Cluster with center FINAL REGULAR COMPUTE:\n"+str(kPoints[centroid[0]]) + ", " + str(list(centroid[1]))) 

print("Final centers: " + str(kPoints))
sc.stop()



#NOTE: this converts an rdd into a normal python ds
#data3 = data2.take(10)
#print(data3)
#print(parseVector("hi,dude")a
