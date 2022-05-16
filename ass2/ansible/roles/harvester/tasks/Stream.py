#
#Team:15
#Yuqi Cao 1186642
#Yaxuan Huang 1118801
#Meng Yang 1193990
#Gangdan Shu 1239032
#Zheng Xu 1291694
#
#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import re
import sys
import tweepy
import json
import couchdb
from urllib3.exceptions import ProtocolError


# In[ ]:


consumer_key = "vLNyFbde05FrwZHxp0QyWekGL"
consumer_secret = "Rtf6okRqZhoej2lF1JdJFQmBEqLKuKVI4dX2rLXDxoWUy7uiZF"
access_token = "1514194227810684931-gzb6TRKJzzfcS55e7WdDWivcFbMODV"
access_token_secret = "N2ZyKsas7y0lPAWwjGChgLGOg9l8DzcepmdSjw1uPbpoD"


# In[ ]:

if not re.search( r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$', sys.argv[1], re.M|re.I):
    exit(1)

couchdb_name = "twitter"
couchdb_address = "http://yuqicao:7486ocean@{}:5984".format(sys.argv[1])
couchdb_server = couchdb.Server(couchdb_address)


# In[ ]:


if couchdb_name in couchdb_server:
    db = couchdb_server[couchdb_name]
else:
    db = couchdb_server.create(couchdb_name)


# In[ ]:


au_bound_box = [113.3390134, -43.6347236, 153.5693633, -10.6680999]
# Citys in melbourne, Sydney and Canberra
idlist = ['01096c751debd6e4',  '01dc11312a909502','0174414074876aed','0099f49a1e9468fa','0073b76548e5984f','01864a8a64df9dc4','066b94a60af1c21d', '01bd76d80f0324ea' , '01eabd04246fc7c6','0060c5e74a64c473', '587e66303af8a504', '7d3e345e5d8fdfc7', '0afae18a56e6352b','78e3a3e78bf72396',  '2656dff1df4fa2f8','045d073d0dd753e5', '46c439e31d6691c7',  '5280716226da86bb', '2b9c9de43d3b163c','4caa1cb3fba85a4e', '746cd6894cb2f59c']  


# In[ ]:


# Subclass Stream to print IDs of Tweets received
class Printer(tweepy.Stream):   
    def on_data(self, data):
        tweet = json.loads(data)
        tweet_id = tweet["id_str"]
        print(tweet_id)
        if tweet_id not in db and tweet["place"]["id"] in idlist:
            if not "extended_tweet" in tweet:
                db[tweet_id] = {"time": str(tweet["created_at"]), "text": tweet["text"], "place_id": tweet["place"]["id"]}
            else:
                db[tweet_id] = {"time": str(tweet["created_at"]), "text": tweet["extended_tweet"]["full_text"], "place_id": tweet["place"]["id"]}
        return True


# In[ ]:


# Initialize instance of the subclass
printer = Printer(
  consumer_key, consumer_secret,
  access_token, access_token_secret
)


# In[ ]:


# Filter realtime Tweets by location
while True:
    try:
        printer.filter(locations = au_bound_box)
    except ProtocolError as PE:
        continue        

