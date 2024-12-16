
"""
1. Consommation d'API: API GOUV -> Liste de villes (communes dont la population > 100.000)
2. Nettoyage de données: (accents, caractère spéciaux, espace, majuscules...)
3. Créer un BDs Relationnelle:
    -> table ville: id, nom
    -> table rapport journalier: id, temp_max, temp_min, date, id_ville(clé étrangère)
    -> Trouver la fonction qui permet de faire : fonction("éèùê", "eeue")
    -> alimenter la table ville
4. Scraping (Site Météo): https://fr.tutiempo.net/paris.html
5. Alimenter notre table "rapport journalier"
 
6. Nice to Have: Extraire les données en (CSV, JSON...) -> les Visualiser avec POWER BI

"""
import requests
from bs4 import BeautifulSoup
import unicodedata
import sqlite3
from bs4 import BeautifulSoup
import pandas as pd
import csv

url = "https://geo.api.gouv.fr/communes"
response = requests.get(url)

if response.status_code == 200:
    villes = response.json()
    grandes_villes = []
    for ville in villes:
        for cle in ville.keys():
            if cle == "population":
                if (ville["population"] > 100000 and ville["codeDepartement"] != "974"):
                    ville["nom"] = ville["nom"].lower().replace(" ","-")
                    ville["nom"] = unicodedata.normalize('NFD', ville["nom"])  # NFD : decomposition canonique
                    ville["nom"] = ''.join(c for c in ville["nom"] if unicodedata.category(c) != 'Mn')
                    #print(f"le code de la ville {ville['nom']} est : (Code : {ville['code']} ) population : {ville['population']}")
                    grandes_villes.append(ville)
    
    #print(len(grandes_villes))
else:
    print(f"Erreur : {response.status_code}")




connexion = sqlite3.connect("meteoDB.db")
#connexion.execute("CREATE TABLE ville(id INTEGER PRIMARY KEY AUTOINCREMENT, nom TEXT not null);")
#connexion.execute("CREATE TABLE rapport_journalier(id INTEGER PRIMARY KEY AUTOINCREMENT, temp_max INTEGER, temp_min INTEGER, date TEXT, id_ville INTEGER, FOREIGN KEY (id_ville) REFERENCES ville(id));")

#for v in grandes_villes:
#    connexion.execute(f"INSERT INTO ville(nom) VALUES('{v['nom']}');")
#    connexion.commit()



journaux = []
for ville in grandes_villes:
        ville_name= ville["nom"]
        result = connexion.execute(f"SELECT id FROM ville WHERE nom = '{ville_name}'")
        for row in result:
            id_ville = row[0]
        
        url = f"https://fr.tutiempo.net/{ville_name}.html"
        response = requests.get(url)
        if response.status_code == 200:
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            for i in range(1,8):
                div = soup.select_one(f"div.dn{i}")
                element = div.select("i")
                #print(div.text.strip() , end=" ")
                date = element[0].text
                temperature_min = element[2].text.split("°")[0]
                temperature_max = element[1].text.split("°")[0]
                #print(f"la ville de {ville_name} Date : [{date}], Température Min : [{temperature_min}], Température Max : [{temperature_max}]    idville = {id_ville}")
                journaux.append({"max" : f"{temperature_max}", "min" : f"{temperature_min}","date" : f"{date}", "id_ville" : f"{id_ville}"})      
        else:
            print(f"Echec de la requette. code HTTP : {response.status_code} ")

#for journal in journaux:
#    connexion.execute(f"INSERT INTO rapport_journalier(temp_max, temp_min, date, id_ville) VALUES ('{journal['max']}', '{journal['min']}', '{journal['date']}', '{journal['id_ville']}');")
#    connexion.commit()




c = connexion.execute(f"SELECT * FROM ville;")
for cr in c:
    print(cr[0],cr[1])


with open('donnees_meteo.csv', 'w', newline='') as file:
    csv_writer = csv.writer (file, delimiter=',', lineterminator='\r\n') #\r retour à la ligne, \n nouvelle ligne
    # ECRIRE DANS LE CSV
    csv_writer.writerow(['Id','Date', 'Température Max', 'Température Min', 'Ville'])
    cursor = connexion.execute(f"SELECT journal.id, journal.date, journal.temp_max, journal.temp_min, v.nom FROM rapport_journalier journal JOIN ville v ON v.id = journal.id_ville ;")
    for ligne in cursor:
        csv_writer.writerow(ligne)
