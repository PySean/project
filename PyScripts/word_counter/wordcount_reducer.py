#!/usr/bin/env python

import sys

current_word = None
word_count = 0
total_words = 0

for line in sys.stdin:
    line = line.strip()
    the_line = line.split("\t")

    if the_line[0] != current_word:
        if current_word is not None:
            print("{}\t{}".format(current_word, word_count))
        total_words += word_count
        word_count = 1
        current_word = the_line[0]
    else:
        word_count += int(the_line[1])
else:
    print("{}\t{}".format(current_word, word_count))
    #print("{}\t{}".format("Total number of words:", total_words))
