#!/usr/bin/env python3
from pyvirtualdisplay import Display
from selenium import webdriver
import feedparser

display = Display(visible=0, size=(800, 600))
display.start()

browser = webdriver.Firefox()
browser.get('http://www.python.org')
print (browser.title)
browser.close()
display.stop()
