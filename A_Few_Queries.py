#!/usr/bin/env python
# coding: utf-8

# In[13]:


# The following script has to be run in CMD or Terminal 
# Change the DB name and Collection name to match your own. 
# -> mongoimport --drop --db [TwitterDB_Connection] --collection [tweets] --type csv --file training.1600000.processed.noemoticon.csv --fields "polarity,id,date,query,user,text" 

# Import pymongo to connect to MongoDB via. Robo 3T
import pprint
import json 
import pymongo
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017")
db = client.TwitterDB_Connection #Paste own DB to test queries =)
tweets = db.tweets
print("Done")


# In[10]:


#Question 1: 
# How many Twitter users are in the database? 
db.tweets.distinct("user") 
print(len(tweets.distinct("user")))


# In[35]:


#Question 2: Twitter user that links the most to other Twitter users.
# Kaspers solution didn't work, so had to re-write, lol. 
ppall(tweets.aggregate([
        {'$match':{'text':{'$regex':'@\w+'}}},
        {'$group':{'_id':'$user',"l":{'$sum':1}}},
        {'$sort':{"l": -1}},
        {'$limit': 10},
    ]))

