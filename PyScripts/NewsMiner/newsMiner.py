#!/usr/bin/env python3
'''
Overview
- Get list of most recent CNN Articles from RSS Feed: 'http://rss.cnn.com/rss/cnn_latest.rss'
- Append any articles we haven't downloaded on a previous run to "articles.txt" file
- Save the timestamp of the newest article we downloaded to "log.txt" to reference next time we run

Prerequisite Software
- Python3
- sudo apt-get install firefox
- sudo apt-get install xvfb
- geckodriver
 - wget https://github.com/mozilla/geckodriver/releases/download/v0.15.0/geckodriver-v0.15.0-linux64.tar.gz
 - tar -xvzf geckodriver-v0.15.0-linux64.tar.gz
 - sudo cp geckodriver /usr/bin/
- sudo apt-get install python3-pip
- sudo pip3 install pyvirtualdisplay
- sudo pip3 install selenium
- sudo pip3 install feedparser

Current RSS Feeds
- http://rss.cnn.com/rss/cnn_latest.rss
- possible improvement: add more feeds

Script info
- Directory: /home/hduser/PyScripts/NewsMiner
- FileName: "newsMiner.py" (Note: Run with Python3)
- Input: "log.txt" - Holds dictionary of the timestamp of the most recent article downloaded from each RSS feed
- Output: Appends the following article info to "articles.txt" with one entry per line 
 - Date(epoch) \t URL \t Title \t Full Article Text
- possible improvement: make script more robust to handle not being able to open it's files and tcp timeouts
- possible improvement: make the script more flexible with CLAs
- possible improvement: program script to read more RSS feeds

Script Automation
- bash script: "repeatScript.sh"
- Runs the "newsMiner.py" script and then sleeps for 600s
- Redirects the script's output to "progress.txt"
- Running in tmux window
 - To attach: "tmux a -t 0" as hduser
 - To detach: "tmux detach" when logged into window
 - possible improvement: turn this into a cron job that runs every 15 mins 
'''
from pyvirtualdisplay import Display #for virtual display to run firefox headless
from selenium import webdriver #to control browser with a script
import feedparser #to parse RSS feeds
import calendar #to convert times to epoch to simplify storage

#retreives a dictionary from specified log file.
#this dictionary stores the most recent timestamp retreived from each RSS feed
def get_log_dictionary(log_file_name):
    log_file = open(log_file_name, "r")
    log_file_text = log_file.read()
    log_file.close()
    return eval(log_file_text)

#writes the dictionary to a file for use the next time the script runs
def write_log_file(log_file_name, log_dict):
    log_file = open(log_file_name, "w+")
    log_file.write(str(log_dict))
    log_file.close()
    return;

#process a single link to an article at cnn.com
def process_cnn_article(browser, articles_file, rss_entry, date):
    url = rss_entry.id #get article url from RSS entry
    print ("Retreiving CNN Article at:", url)
    browser.get(url) #browse firefox to the article
    #get the title of the webpage and remove whitespace that would interfere with our storage
    title = browser.title.replace('\t', ' ').replace('\n', ' ')
    print ("Title:",  title)
    print ("Finding Article Tags")
    #get all tags with a specific class name
    interesting_tags = browser.find_elements_by_class_name("zn-body__paragraph")
    print ("Aggregating Article Text")
    article_text = "" #this loop concatenates all the text from all body tags
    for tag in interesting_tags:
        article_text += tag.text
        article_text += " "
    #remove whitespace that would interfere with our storage format
    article_text = article_text.replace('\t', ' ').replace('\n', ' ') + "\n"
    #format the entry for this article for placement in text file
    s = str(date) + '\t' + url + '\t' + title + '\t' + article_text
    print ("Writing Article to file")
    articles_file.write(s) #write this article to the file
    return;

#process a single link from money.cnn.com
def process_cnn_money_article(browser, articles_file, rss_entry, date):
    url = rss_entry.id #grab article link from RSS entry
    print ("Retreiving CNN Money Article at:", url)
    browser.get(url) #browse firefox to the article
    #get title of article and remove whitespace which would disturb our storage formatting
    title = browser.title.replace('\t', ' ').replace('\n', ' ')
    print ("Title:",  title)
    print ("Finding Article Tag")
    #grab the tag specified by id which contains all the text for the article
    interesting_tag = browser.find_element_by_id("storytext")
    print ("Aggregating Article Text")
    #remove whitespace from article which would interfere with our tab delimited formatting
    article_text = interesting_tag.text.replace('\t', ' ').replace('\n', ' ') + "\n"
    #format output string 
    s = str(date) + '\t' + url + '\t' + title + '\t' + article_text
    print ("Writing Article to file")
    articles_file.write(s) #write article to output file
    return;

#This function determines which articles we have not already downloaded on a previous run and downloads them
def get_latest_cnn_articles(browser, articles_file, log_dict):
    print ("Parsing CNN News Feed")
    cnn_rss_url = 'http://rss.cnn.com/rss/cnn_latest.rss' #url for RSS feed
    d = feedparser.parse('http://rss.cnn.com/rss/cnn_latest.rss') #parse RSS feed
    #record the timestamp of the newest article in the feed
    ts_new = calendar.timegm(d.entries[0].published_parsed)
    ts_log = log_dict[cnn_rss_url] #get timestamp of the newest article we've previously downloaded
    if ts_new <= ts_log: #See if the newest article is newer than we've downloaded before
        print ("No new Articles in CNN RSS Feed")
        return;
    log_dict[cnn_rss_url] = ts_new #update newest article timestamp for reference on next run
    if "money.cnn.com" in d.entries[0].id: #money domain articles have different encoding
        process_cnn_money_article(browser, articles_file, d.entries[0], ts_new)
    else: #must be a normal cnn.com article
        process_cnn_article(browser, articles_file, d.entries[0], ts_new)

    #loop through the rest of the articles and download them until we find one older than (or same age) as what we already downloaded 
    for entry in d.entries[1:]:
        ts_curr = calendar.timegm(entry.published_parsed) #get the timestamp for this article
        if ts_curr <= ts_log: #if this is older than what we already have
            break #then we are done
        if "money.cnn.com" in entry.id: #process money article
            process_cnn_money_article(browser, articles_file, entry, ts_curr)
        else: #process normal article
            process_cnn_article(browser, articles_file, entry, ts_curr)
    return;

#configure virtual display to run firefox "headless"
display = Display(visible=0, size=(800, 600))
display.start()
#open article file and append to it
articles_file = open("articles.txt", "a+")
#start firefox browser
print ("Launching Firefox")
browser = webdriver.Firefox()
#retreive our dictionary of timestamps for the latest articles we've downloaded from each RSS feed
log_dict = get_log_dictionary("log.txt")
#get latest articles from CNNs RSS feed
get_latest_cnn_articles(browser, articles_file, log_dict)
articles_file.close()
write_log_file("log.txt", log_dict) #save our timestamp dictionary so we will know what articles to download next run
print ("Closing FireFox")
browser.close()
display.stop()
