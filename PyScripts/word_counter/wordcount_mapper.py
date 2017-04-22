#!/usr/bin/env python

import re
import sys
import socket

for line in sys.stdin:
    # print('{}\t{}'.format(socket.gethostname(), 0))
    line = line.strip()
    words = line.split("\t") # Tabs separate the first three guys from the words
                         # we care about.
    # Ignore the first  words and grab everything else.
    rest = words[-1]
    # Strip all punctuation and garbage, make lowercase.
    rest = re.sub("[^A-Za-z ]", "", rest).lower()
    for word in rest.split():
        print("{}\t{}".format(word, 1))
