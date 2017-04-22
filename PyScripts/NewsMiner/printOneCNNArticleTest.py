#!/usr/bin/env python3
from pyvirtualdisplay import Display
from selenium import webdriver
import feedparser

display = Display(visible=0, size=(800, 600))
display.start()

print ("Launching Firefox")
browser = webdriver.Firefox()
url = 'http://rss.cnn.com/rss/cnn_latest.rss'
print ("Parsing News Feed:", url)
d = feedparser.parse(url)
browser.get(d.entries[1].id)
print ("Title:", browser.title)
print ("Finding Article Tags")
interesting_tags = browser.find_elements_by_class_name("zn-body__paragraph")
print ("Aggregating Article Text")
article_text = ""
for tag in interesting_tags:
    article_text += tag.text
    article_text += " "
print (article_text)
print ("Closing FireFox")
browser.close()
display.stop()
