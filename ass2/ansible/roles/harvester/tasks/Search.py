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

# In[5]:


# tweepy 4.9.0
import tweepy
import couchdb
import re
import sys
import json
import datetime


# In[ ]:
if not re.search( r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$', sys.argv[1], re.M|re.I):
    exit(1)

bearer_token = "AAAAAAAAAAAAAAAAAAAAANHMbQEAAAAAT8OSGh7YpX9jBmcg%2B3Qrenvp66g%3DOUpZRlL9ANdDdhn5Gu8lE7T0DsLAZqz72BEP4DExXJw0huUTA7"
auth = tweepy.OAuth2BearerHandler(bearer_token)


# In[ ]:


api = tweepy.API(auth, wait_on_rate_limit=True)


# In[ ]:


couchdb_name = "twitter"
couchdb_address = "http://yuqicao:7486ocean@{}:5984".format(sys.argv[1])  
couchdb_server = couchdb.Server(couchdb_address)


# In[ ]:


if couchdb_name in couchdb_server:
    db = couchdb_server[couchdb_name]
else:
    db = couchdb_server.create(couchdb_name)
    


# In[ ]:


# Citys in melbourne, Sydney and Canberra
idlist = ['01bd76d80f0324ea' , '01eabd04246fc7c6','0060c5e74a64c473', '587e66303af8a504', '7d3e345e5d8fdfc7', '0afae18a56e6352b','78e3a3e78bf72396',  '2656dff1df4fa2f8','045d073d0dd753e5', '46c439e31d6691c7',  '5280716226da86bb', '2b9c9de43d3b163c','4caa1cb3fba85a4e', '746cd6894cb2f59c']  
# Top 10 words used in tweet
query = '"the" OR "i" OR "to" OR "a" OR "and" OR "is" OR "in" OR "it" OR "you" OR "of" place:3f14ce28dc7c4566 lang:en'
# eliminate repetitive data
st_day = datetime.date.today()
start_day = str(st_day)


# In[ ]:


tweets = tweepy.Cursor(api.search_tweets, q = query, until = start_day, lang = 'en', tweet_mode='extended', count=100).items()
for tweet in tweets:
    tweet_id = tweet.id_str
    print(tweet_id)
    if tweet_id not in db and tweet.place.id in idlist:
        db[tweet_id] = {"time" : str(tweet.created_at), "text": tweet.full_text, "place_id": tweet.place.id}

