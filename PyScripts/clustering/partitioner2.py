#!/usr/bin/env python3
import nltk
#from nltk.corpus import wordnet as wn
import nltk
import numpy as np
from nltk.stem import PorterStemmer
from pyspark import SparkContext, SparkConf
import random 
from datetime import datetime
import time
import subprocess
import re
nltk.download('wordnet')
from nltk.stem.wordnet import WordNetLemmatizer
#from stemming.porter2 import stem
tempDist = 1.0

STOP_WORDS = ["a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the"]
def parseVector(line):
    theSplits = line.split(',')
    if ('BURGLARY' in theSplits[1]):
        return (theSplits[5], np.array([1, 0]))
    #We're good with a blanket case because we already filter the other stuff out.
    elif ('THEFT' == theSplits[1]):
        return (theSplits[5], np.array([0, 1]))

    else:
        return ("nothin", np.array([0,0]))

def getArticleText(line):
    theSplits=line.split('\t')
    ID = theSplits[0]
    ArticleText = theSplits[3] +' ' + theSplits[4]
    return (ID, ArticleText)
    


def parseArticleSamplesWithID(line, sampleSize):
    port = PorterStemmer()
    theSplits = line.split('\t')
    ID = theSplits[0]
    title = theSplits[3]
    #theWords = []
    theWords = re.sub("[^A-Za-z ]", "", theSplits[4]).split()
    #theWords = re.sub("[^A-Za-z ]", "", theWords)
    histogram = dict()
    #for i in range(sampleSize):
    #    theWords.append(random.choice(theSplits[4].split()))
    


    lemmatizer=WordNetLemmatizer()
    port = PorterStemmer()
    

    
    for i,v in enumerate(theWords):
        if v not in STOP_WORDS:
            try:
                theWords[i] = port.stem(lemmatizer.lemmatize(theWords[i].lower()))
            except:
                brokeStemmer = True
    for word in list(set(theWords)):
        if word not in STOP_WORDS:
            histogram[word] = 0
    for word in theWords:
        if word not in STOP_WORDS:
            histogram[word]+=1
    for key in set(histogram.keys()):
        histogram[key] = float(histogram[key]) / float(len(theWords)) #term frequency 
    return (ID, histogram, title)


def parseArticle(line):
    theSplits = line.split('\t')
    theWords = re.sub("[^A-Za-z ]", "", theSplits[4]).split()
    #theWords = re.sub("[^A-Za-z ]", "", theWords)
    histogram=dict()
    lemmatizer = WordNetLemmatizer()
    port = PorterStemmer()
    for i,v in enumerate(theWords):
        if v not in STOP_WORDS:
            try:
                theWords[i] = port.stem(lemmatizer.lemmatize(theWords[i].lower()))
            except:
                brokeStemmer = True
    for word in list(set(theWords)):
        if word not in STOP_WORDS:
            histogram[word]=0
    for word in theWords:
        if word not in STOP_WORDS:
            histogram[word]+=1
    #for key in histogram.keys():
    #    histogram[key] = float(histogram[key])/float(totalHistory[key])
    for key in set(histogram.keys()):
        histogram[key] = float(histogram[key]) / float(len(theWords)) #term frequency
    return histogram


#compare two article vectors with this function
def compareArticles(article1, article2):
    keys1 = set(article1.keys())
    keys2 = set(article2.keys())
    union = keys1 | keys2
    total = 0
    for key in list(union):
        total += (article1.get(key, 0) - article2.get(key,0))**2
    return total
    
def printStuff(line):
    print(line)
    return 0

def merge_two_dicts(x, y):
    """Given two dicts, merge them into a new dict as a shallow copy."""
    newDict = dict()
    keys1 = set(x.keys()) 
    keys2 = set(y.keys())
    union = keys1 | keys2
    for key in list(union):    
        newDict[key] = x.get(key,0) + y.get(key,0)
    return newDict 


def updateCounts(histogram, totalHistory):
    for key in histogram.keys():                                               
        histogram[key] = float(histogram[key])/float(totalHistory[key])  
    return histogram

def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist = compareArticles(p, centers[i])
        if tempDist < closest:
            closest = tempDist 
            bestIndex = i
    return bestIndex

def divDict(histogram, number):
    for key in histogram.keys():
        histogram[key] = histogram[key] / number
    return histogram

def moveToComputeFromStorage(rDDs):
    clustered_HDFS_path = '/user/hduser/clusteredArticles/'
    subprocess.call(["/usr/local/hadoop-1.2.1/bin/hadoop", "fs", "-rmr", clustered_HDFS_path])
    clusterCount = 1
    for rDD in rDDs:
        rDD = rDD.repartition(1)
        rDD.saveAsTextFile(clustered_HDFS_path + str(clusterCount) + "/")
        clusterCount += 1
    return

def moveToComputeFromStorage2(rDDs):
   clustered_HDFS_path = '/user/hduser/clusteredArticlesTest/'
   hadoop_location = "/usr/local/hadoop-1.2.1/bin/hadoop"
   subprocess.call([hadoop_location, "fs", "-rmr", clustered_HDFS_path])
   clusterCount = 1
   for rDD in rDDs:
       rDD = rDD.repartition(1)
       outpath = clustered_HDFS_path + str(clusterCount) + '/'
       print (outpath)
       outfile = outpath + 'part-00000'
       newfile = clustered_HDFS_path + 'cluster' + str(clusterCount)
       #subprocess.call([hadoop_location, "fs", "-mkdir", outpath])
       rDD.saveAsTextFile(outpath)
       subprocess.call([hadoop_location, "fs", "-mv", outfile, newfile])
       subprocess.call([hadoop_location, "fs", "-rmr", outpath])
       clusterCount += 1
   return
def moveToComputeFromStorage3(KRDDs):
    clusterNo = 0
    for rDD in KRDDs:
        rDD.saveAsTextFile('/clusteredResults/c'+str(clusterNo))
        clusterNo+=1

#crime type: ndx 1, address: index 5
sc = SparkContext(appName="kmeans")
lines = sc.textFile("hdfs://master:54310/user/hduser/articles").cache()

data = sc.parallelize(lines.takeSample(False, 1000, int(time.time()))).map(parseArticle).cache() 
#data=lines.map(parseArticle).cache()



#data = sc.parallelize(dataWithID.map(lambda x:x[1]).take(1000)).cache()
totalHistory=data.reduce(lambda x, y : merge_two_dicts(x,y))                     
#print(totalHistory) 
#print("\n\nFirst Article"+str(data.collect()[0]))                                        
#data=data.map(lambda histogram : updateCounts(histogram, totalHistory)).cache()



#print("\n\nFirst Article"+str(data.collect()[0]))
#print(compareArticles(data.collect()[1], data.collect()[0]))

SAMPLE_SIZE = 100
tempDist = 1
K = 3
numPoints = data.count()
convergeDist = .001
kPointss = data.takeSample(False, K, random.randint(1,100))
kPoints = []
for kPoint in kPointss:
    kPoints.append(kPoint)
while tempDist > convergeDist:
    closest = data.map(lambda x : (closestPoint(x, kPoints),(x,1))).cache()
    testClosest = closest.groupByKey().collect()
#    print("================NEW ITERATION===============")
    testCount = 0
#    for centroid in testClosest:
#        print("Cluster " + str(testCount) + str(len(list(centroid[1]))))
    pointStats = closest.reduceByKey(lambda p1, p2: (merge_two_dicts(p1[0],p2[0]), p1[1] + p2[1]))
    pointStats.cache()
    newPointss = pointStats.map(lambda st: (st[0], [divDict(st[1][0], st[1][1]), st[1][1]]))
    newPoints=newPointss.collect()
    tempDist = sum(compareArticles(kPoints[Ki],p[0]) for (Ki, p) in newPoints)
    for (Ki, p) in newPoints:                                 
        kPoints[Ki] = p[0]

#print("Final centers: "+str(kPoints))

dataSamples=lines.map(lambda x : parseArticleSamplesWithID(x,SAMPLE_SIZE))
#dataSamplesNotID=dataSamples.map(lambda x : x[1])
#totalSampleHistory = dataSamplesNotID.reduce(lambda x, y : merge_two_dicts(x,y))
#dataSamples=dataSamples.map(lambda (ID, histogram): (ID, updateCounts(histogram, totalSampleHistory))).cache()
articleIDsWithCluster = dataSamples.map(lambda (ID, histogram, title) : (ID, closestPoint(histogram, kPoints), title)).map(lambda x : (x[1], x[0])).groupByKey().collect()



article=lines.map(getArticleText).cache()
clustNo = 0
KRDDs = []
for (clust, IDs) in articleIDsWithCluster:
    #KRDDs.append(article.filter(lambda x: (x[0] in IDs)).cache())
    '''print("************CLUSTER :***********" + str(clustNo))
    for article in list(clust[1]):
        print(article.encode('utf8'))'''
    clust = article.filter(lambda x: (x[0] in IDs))
    #print(str(len(list(IDs))))
    #print(KRDDs[clustNo].first())
    file1 = open("clust"+str(clustNo), "a")
    for artc in list(clust.collect()):
        file1.write(artc[1].encode('utf8'))
        file1.write('\n')
    file1.close()
    clustNo += 1
rDDNo = 0
'''for rDD in KRDDs:
    file1 = open("clust"+str(rDDNo), "a")
    for article in rDD.collect():
        file1.write(article[1].encode('utf8'))
        file1.write('\n')
    #file1.write(rDD.collect().encode(utf-8))
    file1.close()
    rDDNo+=1
'''



#moveToComputeFromStorage3(KRDDs)
sc.stop()


  
#import sys
#print(sys.version)
''''data = lines.filter(lambda x: ("BURGLARY" in x or "THEFT" in x)).map(parseVector).filter(lambda x:("nothin" not in x[0]))
data2 = data.reduceByKey(lambda x, y : x + y).cache()
#for Testing below
print(range (0, 100)+range(350, 450)+range(700,800)+range(1050, 1150))
data2 = data3.filter(lambda x: (x[1][0] in range (0, 100)+range(350, 450)+range(700,800)+range(1050, 1150) and x[1][1] in range(0,100)+range(350, 450)+range(700,800)+range(1050, 1150))).cache()
K = 5
convergeDist = .001

kPointss = data2.takeSample(False, K, 1)
kPoints=[]
#below for testing
#kPointss = [('a', np.array([50, 50])),('b', np.array([400,400])), ('c', np.array([750,750])), ('d', np.array([1100, 1100]))]
for kPoint in kPointss:
    kPoints.append(kPoint[1])
while tempDist > convergeDist:
    #print(kPoints)
    closest = data2.map(lambda (k,v) : (closestPoint(v, kPoints), (v, 1)))
    closest.cache()
    #print(str(closest.collect()[0]) + " and " + str(closest.collect()[1]))
    pointStats = closest.reduceByKey(lambda p1, p2: (p1[0] + p2[0], p1[1] + p2[1]))
    pointStats.cache()
    psc = pointStats.collect()
    #print(str(psc[0][0])+str(psc[0][1]))
    newPointss = pointStats.map(lambda st: (st[0], st[1][0] / st[1][1]))
    newPoints=newPointss.collect()
    #print("finished an action...")
    tempDist = sum(np.sum( (kPoints[iK] - p) ** 2) for (iK, p) in newPoints)
    for (iK, p) in newPoints:
        kPoints[iKd] = p

print("Final centers: " + str(kPoints))
sc.stop()


'''
#NOTE: this converts an rdd into a normal python ds
#data3 = data2.take(10)
#print(data3)
#data.map(printStuff)
#data.saveAsTextFile('hdfs://master:54310/testkmeans.txt')
#print(parseVector("hi,dude")a
