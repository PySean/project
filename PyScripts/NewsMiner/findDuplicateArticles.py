#!/usr/bin/env python3

import time

articles_file = open("articles.txt", "r")

foundArticlesDict = {}
articleCount = 0

for line in iter(articles_file):
    articleCount += 1
    url = line.split("\t")[1]
    date = line.split("\t")[0]
    #if url in foundArticlesDict:
    if foundArticlesDict.get(url) == date:
        print ("duplicate url with same date:", url)
    else:
        foundArticlesDict[url] = date

print("Total Articles:", articleCount, "Unique Articles:", len(foundArticlesDict))
