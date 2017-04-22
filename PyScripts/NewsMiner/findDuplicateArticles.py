#!/usr/bin/env python3

import time

def get_log_dictionary(log_file_name):
    log_file = open(log_file_name, "r")
    log_file_text = log_file.read()
    log_file.close()
    return eval(log_file_text)

log_dict = get_log_dictionary("log.txt")

cnn_rss_url = 'http://rss.cnn.com/rss/cnn_latest.rss' #url for RSS feed

print ("Latest Time Stamp Mined from CNN:", time.strftime('%m/%d/%Y %H:%M:%S',time.gmtime(log_dict[cnn_rss_url])))
