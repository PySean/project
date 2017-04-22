#!/usr/bin/env python
import feedparser
d = feedparser.parse('http://rss.cnn.com/rss/cnn_latest.rss')
#print "Length: %d" % len (d)
#print "Dictionary Keys:"
#print d.keys()
#print "Feed Keys:"
#print d.feed.keys()
#print "d.feed.link"
#print d.feed.link
#print len(d.entries)

print d.entries[0].published_parsed
for entry in d.entries:
    print entry.id
