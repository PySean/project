#!/usr/bin/env python3
from pyspark import SparkContext, SparkConf
import subprocess



def moveToComputeFromStorage(rDDs):
    clustered_HDFS_path = '/user/hduser/clusteredArticles/'
    hadoop_location = "/usr/local/hadoop-1.2.1/bin/hadoop"
    subprocess.call([hadoop_location, "fs", "-rmr", clustered_HDFS_path])
    clusterCount = 1
    for rDD in rDDs:
        rDD = rDD.repartition(1)
        outpath = clustered_HDFS_path + str(clusterCount) + "/"
        #print (outpath)
        outfile = outpath + 'part-00000'
        #print (outfile)
        newfile = clustered_HDFS_path + 'cluster' + str(clusterCount)
        #print(newfile)
        #subprocess.call([hadoop_location, "fs", "-mkdir", outpath])
        rDD.saveAsTextFile(outpath)
        subprocess.call([hadoop_location, "fs", "-mv", outfile, newfile])
        subprocess.call([hadoop_location, "fs", "-rmr", outpath])
        clusterCount += 1
    return

'''
def moveToComputeFromStorage(rDDs):
    clustered_HDFS_path = '/user/hduser/clusteredArticles/'
    subprocess.call(["/usr/local/hadoop-1.2.1/bin/hadoop", "fs", "-rmr", clustered_HDFS_path])
    clusterCount = 1
    for rDD in rDDs:
        rDD = rDD.repartition(1)
        rDD.saveAsTextFile(clustered_HDFS_path + str(clusterCount) + "/")
        clusterCount += 1
    return
'''


sc = SparkContext(appName="fileClusterTest")

lines = sc.textFile("hdfs://master:54310/user/hduser/clusteredArticles/")
#lines = sc.textFile("hdfs://master:54310/user/hduser/articles/")

#print (lines.count())

rDDs = []
rDDs.append(lines.sample(False, 0.1, 81))
rDDs.append(lines.sample(False, 0.1, 24))
rDDs.append(lines.sample(False, 0.1, 17))
moveToComputeFromStorage(rDDs)

#print (lines.getNumPartitions())
