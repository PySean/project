import numpy as np
from random import randint
import sys

listOfNum = []
weights = np.array([.5, 2, 4, .25, 1], dtype=np.float32)/5
for i in range(0,200):
    for i in range (0, 5):
        myInt = randint(0,9)
        listOfNum.append(myInt)
        sys.stdout.write(str(myInt)+";")
    wtx = np.array(listOfNum, dtype=np.float32).dot(weights.T)
    if np.average(wtx) < 8:
        #sys.stdout.write(str(wtx)+str(0)+'\n')
        print(0)
    else:
        #sys.stdout.write(str(wtx)+str(1)+'\n')
        print(1)
    listOfNum=[]
