#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 09:10:59 2018

@author: alka
"""

###Import necessary packages
import pymongo
from pymongo import MongoClient
import pandas as pd

import matplotlib as mpl
import matplotlib.pyplot as plt
mpl.style.use(["ggplot"])



###Connecting Pymongo client to MongoDB and creating the database
clientMongoDB = MongoClient('mongodb://localhost:27017/')
db = clientMongoDB["OpendataNY"]


##Retrieve collection of requests from MongoDB
requestsCollection = db["Requests"]

##Retrieve collection of police complaints from MongoDB
complaintsCollection = db["policeComplaints"]

##Retrieve collection of water quality from MongoDB
WaterQualityCollection = db["WaterQuality"]

###loading  Housing data from MongoDB
housingCollection = db["Housing"]



############################################################################
#***I. Statistiques sur les plaintes 311 dans la ville de New York*********#
############################################################################
#-----1... Quel Borough a enregistré le plus de plaintes
complaints  = requestsCollection.aggregate([
        {"$match":{"borough":{"$exists":True, "$ne":"Unspecified"}}},
        {"$group":{"_id":"$borough", 
                   "nb_plaintes":{"$sum":1} } },
        {"$sort":{"nb_plaintes":-1}},
        {"$project":{"nb_plaintes":1, "Borough":"$_id", "_id":0}}
        ])

df_complaints = pd.DataFrame(list( complaints))
df_complaints.set_index("Borough", drop=False, inplace=True)
df_complaints.index.name = None
print("Le Borough qui a enregistré le ***PLUS*** de plaintes au niveau du service 311 est : \"{}\"".format(
        df_complaints["Borough"].iloc[0]))
print("Le Borough qui a enregistré le ***MOINS*** de plaintes au niveau du service 311 est : \"{}\"".format(
        df_complaints["Borough"].iloc[4]))

####Graphiques sur le nombre de plaintes par Borough
fig = plt.figure()
plt.title("Frequence des plaintes reçues par le service 311 pour les 5 Borough de NYC")
plt.xlabel("Nombre de plaintes")
plt.ylabel("Borough")
for index, value in enumerate(df_complaints.nb_plaintes): 
    label = format(int(value), ',') # format int with commas
    # place text at the end of bar (subtracting 47000 from x, and 0.1 from y to make it fit within the bar)
    plt.annotate(label, xy=(value - 6000, index - 0.10), color='white', size="x-large")
df_complaints["nb_plaintes"].plot(kind="barh", figsize=(10, 10))
fig.savefig('plaintes_par_borough.png')

#-----2... Taux de plaintes pour bruits parmi 
##########les plaintes reçues par le service 311 : dont la descritpion contient les termes:
########## noisy, noise, nois
complaints_bis  = requestsCollection.aggregate([
        {"$match":{"borough":{"$exists":True, "$ne":"Unspecified"},
                   "descriptor":{"$regex":"(.*noise.*|.*noisy.*|.*nois.*)", "$options":"i"}}},
        {"$group":{"_id":"$borough", 
                   "nb_plaintes_bis":{"$sum":1} } },
        {"$sort":{"nb_plaintes_bis":-1}},
        {"$project":{"nb_plaintes_bis":1, "Borough":"$_id", "_id":0}}
        ])

df_complaints_bis = pd.DataFrame(list( complaints_bis ))
df_complaints_bis.set_index("Borough", drop=False, inplace=True)
df_complaints_bis.index.name = None
df_complaints = df_complaints.merge(df_complaints_bis).set_index("Borough")
df_complaints["taux_bruit"] = round(100*(df_complaints["nb_plaintes_bis"]/df_complaints["nb_plaintes"]), 2)
df_complaints.sort_values("taux_bruit", inplace=True, ascending=False)
df_complaints["Borough"] = df_complaints.index.values

print("Le Borough qui a ***La plus grande part de plaintes pour bruit*** est : \"{}\"".format(
        df_complaints["Borough"].iloc[0]))
print("Le Borough qui a ***La plus faible part de plaintes pour bruit*** est : \"{}\"".format(
        df_complaints["Borough"].iloc[4]))

####Graphiques sur le nombre de plaintes par Borough
fig = plt.figure()
plt.title("Taux des plaintes pour bruit reçues par le service 311 pour les 5 Borough de NYC")
plt.xlabel("% plaintes pour bruit")
plt.ylabel("Borough")
for index, value in enumerate(df_complaints.taux_bruit): 
    # place text at the end of bar (subtracting 47000 from x, and 0.1 from y to make it fit within the bar)
    plt.annotate(value, xy=(value - 0.4, index - 0.10), color='white', size="x-large")
df_complaints["taux_bruit"].plot(kind="barh", figsize=(10, 10))
fig.savefig('./plaintes_taux_bruit.png')


############################################################################
#***II. Statistiques sur les plaintes reçues par la police      ***********#
############################################################################
#-----1... Quel Borough a enregistré le plus de plaintes déposées auprès de la police de NYC
policeComplaints  = complaintsCollection.aggregate([
        {"$match":{"boro_nm":{"$exists":True, "$ne":"Unspecified"}}},
        {"$group":{"_id":"$boro_nm", 
                   "nb_plaintes":{"$sum":1} } },
        {"$sort":{"nb_plaintes":-1}},
        {"$project":{"nb_plaintes":1, "Borough":"$_id", "_id":0}}
        ])

df_police = pd.DataFrame(list( policeComplaints))
df_police.set_index("Borough", drop=False, inplace=True)
df_police.index.name = None
print("Le Borough qui a enregistré le ***PLUS*** de plaintes au niveau de la police est : \"{}\"".format(
        df_police["Borough"].iloc[0]))
print("Le Borough qui a enregistré le ***MOINS*** de plaintes au niveau de la police est : \"{}\"".format(
        df_police["Borough"].iloc[4]))


####Graphiques sur le nombre de plaintes auprès de la police par Borough
fig = plt.figure()
plt.title("Frequence des plaintes reçues par la Police pour les 5 Borough de NYC")
plt.xlabel("Nombre de plaintes")
plt.ylabel("Borough")
for index, value in enumerate(df_police.nb_plaintes): 
    label = format(int(value), ',') # format int with commas
    # place text at the end of bar (subtracting 47000 from x, and 0.1 from y to make it fit within the bar)
    plt.annotate(label, xy=(value - 6000, index - 0.10), color='white', size="x-large")
df_police["nb_plaintes"].plot(kind="barh", figsize=(10, 10))
fig.savefig('./police_plaintes.png')


#-----2... Repartition des plaintes dont la description contient l'un des termes:
#"crim", "drug", "sex", "alcoh", "murder", "kidnap"
policeComplaints_bis  = complaintsCollection.aggregate([
        {"$match":{"boro_nm":{"$exists":True, "$ne":"Unspecified"}, 
                   "ofns_desc":{"$regex":"(.*crim.*|.*drug.*|.*sex.*|.*alcoh.*|.*murder.*|.*kidnap.*)",  "$options": "i"}}},
        {"$group":{"_id":"$boro_nm", 
                   "nb_plaintes_bis":{"$sum":1} } },
        {"$sort":{"nb_plaintes_bis":-1}},
        {"$project":{"nb_plaintes_bis":1, "Borough":"$_id", "_id":0}}
        ])

df_police_bis = pd.DataFrame(list( policeComplaints_bis ))
df_police_bis.set_index("Borough", drop=False, inplace=True)
df_police_bis.index.name = None
df_police = df_police.merge(df_police_bis).set_index("Borough")
df_police["taux_crime"] = round(100*(df_police["nb_plaintes_bis"]/df_police["nb_plaintes"]), 2)
df_police.sort_values("taux_crime", inplace=True, ascending=False)
df_police["Borough"] = df_police.index.values

print("Le Borough qui a  ***La plus grande part*** part de plaintes sérieuses enregistrées par la police est : \"{}\"".format(
        df_police["Borough"].iloc[0]))
print("Le Borough qui a ***La plus faible part*** part de plaintes sérieuses enregistrées par la police est : \"{}\"".format(
        df_police["Borough"].iloc[4]))

####Graphiques sur le nombre de plaintes auprès de la police par Borough
fig = plt.figure()
plt.title("Taux de plaintes sérieuses reçues par la Police pour les 5 Borough de NYC")
plt.xlabel("% de plaintes \"sérieuses\" ")
plt.ylabel("Borough")
for index, value in enumerate(df_police.taux_crime): 
    plt.annotate(value, xy=(value - 2, index - 0.10), color='white', size="x-large")
df_police["taux_crime"].plot(kind="barh", figsize=(10, 10))
fig.savefig('./taux_plaintes_serieuses_police.png')

print("""***************************************************************************\n
****Borough retenu: QUEENS (plus faible taux de crimes graves)  ***********\n
***************************************************************************\n""")


#####################################################################################################
#*** III. recupérer les documents correspondant à QUEENS et chercher la résidence idéale  **********#
#####################################################################################################


####Convertir les longitudes et latitude en numérique
db.eval("""db.Housing.find( ).forEach( function (x) { x.latitude = parseFloat(x.latitude);
                           x.longitude = parseFloat(x.longitude);
                           db.Housing.save(x);})""")

###Supprimer les valeurs non valides pour les coordonnées géographiques
db.Housing.delete_many(
        {
                "$or":[{"latitude":float('nan')}, 
                              {"longitude":float('nan')}]
    }
    )

####Créer un champ borough à partir du code "boro" dans la collection housing
db.eval("""
        db.Housing.find( ).forEach( function (x) { 
        
        switch (x.boro) {
            case '1':
                x.borough = "MANHATTAN";
                break;
            case '2':
                x.borough = "BRONX";
                break;
            case '3':
                x.borough = "BROOKLYN";
                break;
            case '4':
                x.borough = "QUEENS";
                break;
            case '5':
                x.borough = "STATEN ISLAND";
                break;
            default:
                x.borough = null;
        }
        db.Housing.save(x);
        })
        
        """
    )


####Supprimer les données qui ne sont pas du Borough QUEENS
db.Housing.delete_many(
        {
            "borough":{"$ne":"QUEENS"}
    }
    )

####Création du champ coord (long, lat) puis  d'un index géographique
db.Housing.aggregate(
    [
     {"$match":{"longitude":{"$exists":True}, "latitude":{"$exists":True}} },
        { "$addFields": { 
            "coord": ["$longitude", "$latitude"] 
        }},
        { "$out": "Housing" }
    ]
)

db.Housing.create_index([("coord", pymongo.GEO2D)])


####Quelques traitements sur la base des plaintes pour qualité de l'eau
####Convertir les longitudes et latitude en numérique
db.eval("""db.WaterQuality.find( ).forEach( function (x) { x.latitude = parseFloat(x.latitude);
                           x.longitude = parseFloat(x.longitude);
                           db.WaterQuality.save(x);})""")

###Supprimer les documents qui n'ont pas de champ longitude ou latitude
db.WaterQuality.delete_many(
        {
                "$or":[{"latitude":float('nan')}, 
                              {"longitude":float('nan')}]
    }
    )

    
###Créer le champ coord dans la collection waterquality et créer un index géographique basé sur ce champ
db.WaterQuality.aggregate(
    [
     {"$match":{"longitude":{"$exists":True}, "latitude":{"$exists":True}} },
        { "$addFields": { 
            "coord": ["$longitude", "$latitude"] 
        }},
        { "$out": "WaterQuality" }
    ]
)

db.WaterQuality.create_index([("coord", pymongo.GEO2D)])


#####Pour chaque résidence chercher le nombre de plaintes pour mauvaise qualité d'eau dans un rayon de 5 Km
##diviser par 6378.1 pour convertir de Km en radians
db.eval("""
            db.Housing.find( ).forEach( function (x) {
            var point = x.coord;
            var dist = 5/6378.1; 
            var result = db.Housing.find( { "coord": { "$geoWithin": { "$centerSphere": [point ,dist ] } } } );
            var compt = result.count();
            x.nombre_plaintes_eau = compt;
            db.Housing.save(x);
                }
            )
        """
        )

#########################################################################################################
#*** Finalement quelles sont les 5 résidenceS avec le moins de plaintes dans un rayon de 5Km ***********#
#########################################################################################################
residences_Ideales = pd.DataFrame( list(
        db.Housing.find(
            limit=5,
            projection={"borough":1, "lot":1, "coord":1, "parcel_address":1, "postcode":1, "nombre_plaintes_eau":1}
            ).sort("nombre_plaintes_eau", pymongo.ASCENDING)
        )
        )

print("""***************************************************************\n
** Les 5 meilleures résidences avec le moins de plaintes ******\n
** pour la qualité de l'eau dans un rayon de 5 kilomètres sont:\n
***************************************************************\n
	""")
print(residences_Ideales)
