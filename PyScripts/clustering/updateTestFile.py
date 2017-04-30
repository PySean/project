
file1 = open("articles1.txt", "r")
file2 = open("testFile2", "w")
lineNo = 1
for line in file1:
    pieces = line.split('\t')
    file2.write(str(lineNo))
    for piece in pieces:
        file2.write("\t"+str(piece))
    lineNo += 1


file1.close()
file2.close()
