file1 = open("testFile2", "r")
lineNo = 0
for line in file1:
    testTabs = line.split('\t')
    if lineNo == 0:
        for testTab in testTabs:
            print(testTab)
    lineNo+=1
