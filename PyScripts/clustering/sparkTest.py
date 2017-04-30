#!/usr/bin/env python
from __future__ import print_function
from pyspark import SparkContext, SparkConf

sc = SparkContext(appName="Spark Test")

lines = sc.textFile("hdfs://yarnmaster/user/cc/kmeansinput")
print("Total Lines: ", lines.count())
#lines.take(5)
#lines.getNumPartitions()
#lines.filter(lambda line: "Incident" not in line).count()

#print("Hello Cloud")
