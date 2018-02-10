#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 28 10:43:56 2018

@author: alka
"""

###Import de modules nécessaires
from sodapy import Socrata
from pymongo import MongoClient




###Connexion d'un client à l'API du site Open Data, je précise l'option timeout=3600 pour avoir
#un délai d'une heure avant de lever une exception 'Timeout exception'
##Ceci afin de tolérer meme les connexions bas débit
clientNYC = Socrata("data.cityofnewyork.us", None, timeout=3600)

#### Téléchargement des données relatives aux plaintes déposées au niveau du
## 311 Service Requests (plaintes non urgentes): 
## 200.000 plus recentes requêtes entre 1er janv 2017 et 27 janv 2018
requests_311 = clientNYC.get("fhrw-4uyv", 
                      where="created_date < '2018-01-27T23:59:59.000' AND created_date >= '2017-01-01T00:00:00.000'",
                      order="created_date", limit=200000)


#### Données relatives aux plaintes déposées auprès de la Police de New York
## : Les 200.000 plus récentes plaintes enregistrées par la police
complaints_NYPD = clientNYC.get("9s4h-37hy", order="rpt_dt DESC", limit=200000)


####Données relatives aux plaintes déposées concernant la qualité de l'eau à New York
## du 1er Janv 2010 au 26 janv 2018 : 9504 enregistrements
water_Quality = clientNYC.get("qfe3-6dkn", order="created_date ASC", limit = 9504)


##Données relatives aux logements avec leurs adresses dans la ville de New York 
housingData = clientNYC.get("m5g2-927e", limit = 1000000)


###Connexion d'un client à la BDD mongodb et création d'une base de données 'OpendataNY'
clientMongoDB = MongoClient('mongodb://localhost:27017/')
db = clientMongoDB["OpendataNY"]


## Création de la collection des requêtes du service 311 
## et insertion des données dans cette collection
requestsCollection = db["Requests"]
insertedRequests = requestsCollection.insert_many(requests_311)


## Création de la collection des plaintes auprès de la police 
## et insertion des données dans cette collection
complaintsCollection = db["policeComplaints"]
insertedComplaints = complaintsCollection.insert_many(complaints_NYPD)


## Création de la collection des plaintes pour qualité de l'eau 
## et insertion des données dans cette collection
WaterQualityCollection = db["WaterQuality"]
insertedWaterQuality = WaterQualityCollection.insert_many(water_Quality)

## Création de la collection des logements 
## et insertion des données dans cette collection
housingCollection = db["Housing"]
insertedGeoData = housingCollection.insert_many(housingData)

print("****************Données téléchargées et insérées dans MongoDB avec succès!!!**********\n")

#### Fermeture des connexions ouvertes
clientNYC.close()
clientMongoDB.close()
print("*********Connexions fermées avec succès!!!*********\n")

