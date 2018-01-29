#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 28 10:43:56 2018

@author: alka
"""

###Import necessary packages
from sodapy import Socrata
from pprint import pprint
from pymongo import MongoClient




###Connect the client to the website, with one hour delay 
##before raising the Timeout exception
clientNYC = Socrata("data.cityofnewyork.us", None, timeout=3600)

#### NYC 311 Service Requests : 200.000 most recents requests between  jan 1 2017 and jan 27 2018
requests_311 = clientNYC.get("fhrw-4uyv", 
                      where="created_date < '2018-01-27T23:59:59.000' AND created_date >= '2017-01-01T00:00:00.000'",
                      order="created_date", limit=200000)


#### NYPD Complaint Data Historic : the 200.000 most recent recorded incidents by Police
#https://data.cityofnewyork.us/resource/9s4h-37hy.json

complaints_NYPD = clientNYC.get("9s4h-37hy", order="rpt_dt DESC", limit=200000)




####Water quality data for NYC, from Jan 1st 2010 to Jan 26 2018 : 9504 records
##https://data.cityofnewyork.us/resource/qfe3-6dkn.json
water_Quality = clientNYC.get("qfe3-6dkn", order="created_date ASC", limit = 9504)

###Bus Breakdown and Delays take all the 192.000 data available on jan 28 2018
#https://data.cityofnewyork.us/resource/fbkk-fqs7.json
#bus_Breakdown = clientNYC.get("fbkk-fqs7", limit=191667)



###Connecting Pymongo client to MongoDB and creating the database
clientMongoDB = MongoClient('mongodb://localhost:27017/')
db = clientMongoDB["OpendataNY"]


##Create collection for requests data and load data into it
requestsCollection = db["Requests"]
insertedRequests = requestsCollection.insert_many(requests_311)


##Create collection for Police complaints data and load data into it
complaintsCollection = db["policeComplaints"]
insertedComplaints = complaintsCollection.insert_many(complaints_NYPD)


##Create collection for Water quality data and load data into it
WaterQualityCollection = db["WaterQuality"]
insertedWaterQuality = WaterQualityCollection.insert_many(water_Quality)


#####Print some results
myCollection = [requestsCollection, complaintsCollection, WaterQualityCollection]
for collect in myCollection:
    print("\n***************************************************\n")
    pprint(collect.find_one())
    


    








#results4 = client.get("fhrw-4uyv", order="created_date DESC", limit=100000)
#results_df4.shape
#results_df4.head()
#results_df4.columns
#results_df4.created_date[:10]
#metadata = client.get_metadata("erm2-nwe9")











