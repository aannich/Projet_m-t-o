
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

# Importation des bibliothèques
import requests
from bs4 import BeautifulSoup
import unicodedata
import sqlite3
from bs4 import BeautifulSoup
import pandas as pd
import csv

# fonction nettoyage pour nettoyer les noms des ville et des regions  (enlève accents, espaces, met en minuscules)               
def nettoyage(nom):
    nom = nom.lower().replace(" ", "-")
    nom = unicodedata.normalize('NFD', nom)  # NFD Décomposition canonique
    nom = ''.join(c for c in nom if unicodedata.category(c) != 'Mn')
    return nom

# Appel de l'API pour récupérer la liste des ville
url = "https://geo.api.gouv.fr/communes"
response = requests.get(url)
if response.status_code == 200:
    villes = response.json()
    grandes_villes = []
    for ville in villes:
        for cle in ville.keys():
            if cle == "population":
                if (ville["population"] > 100000 ):
                    ville["nom"] = nettoyage(ville["nom"])
                    grandes_villes.append(ville)
else:
    print(f"Erreur : {response.status_code}")

connexion = sqlite3.connect("meteoDB.db")
connexion.execute("CREATE TABLE IF NOT EXISTS ville(id INTEGER PRIMARY KEY AUTOINCREMENT, ville TEXT not null, code_region INTEGER, FOREIGN KEY (code_region) REFERENCES region(code) );")
connexion.execute("CREATE TABLE IF NOT EXISTS rapport_journalier(id INTEGER PRIMARY KEY AUTOINCREMENT, temp_max INTEGER, temp_min INTEGER, date TEXT, id_ville INTEGER, FOREIGN KEY (id_ville) REFERENCES ville(id));")
connexion.execute("CREATE TABLE IF NOT EXISTS region (code INTEGER PRIMARY KEY, region TEXT);")

for v in grandes_villes:
    connexion.execute(f"INSERT INTO ville(ville,code_region) VALUES('{v['nom']}','{v['codeRegion']}');")
connexion.commit()

journaux = []
for ville in grandes_villes:
        ville_name= ville["nom"]
        result = connexion.execute(f"SELECT id FROM ville WHERE ville = '{ville_name}'")
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

# Alimenter la table rapport_journalier
for journal in journaux:
    connexion.execute(f"INSERT INTO rapport_journalier(temp_max, temp_min, date, id_ville) VALUES ('{journal['max']}', '{journal['min']}', '{journal['date']}', '{journal['id_ville']}');")
    connexion.commit()



# le fichier csv qui contient la liste des regions avec le code de la region et telecherger 
# du site de l'institut national de la statistique et des études économiques 
#  https://www.insee.fr/fr/information/6051727
with open('region_2022.csv', newline='') as file:
    file.readline()  # Ignore la première ligne (en-têtes)
    csv_reader = csv.reader(file, delimiter=',')
    for ligne in csv_reader:
        code = ligne[0]
        nom = nettoyage(ligne[3])
        # Alimenter la table region
        connexion.execute(f"INSERT INTO region (code, region) VALUES ({code}, '{nom}');")
        connexion.commit()
    
with open('donnees_meteo.csv', 'w', newline='') as file:
    csv_writer = csv.writer (file, delimiter=',', lineterminator='\r\n') #\r retour à la ligne, \n nouvelle ligne
    # ecrire dans le fichier csv
    csv_writer.writerow(['Id','Date', 'Température Max', 'Température Min', 'Ville', 'Region'])
    cursor = connexion.execute(f"SELECT j.id, j.date, j.temp_max, j.temp_min, v.ville, r.region FROM rapport_journalier j JOIN ville v on j.id_ville=v.id  JOIN region r ON r.code = v.code_region;")
    for ligne in cursor:
        csv_writer.writerow(ligne)
    file.close()

connexion.close()

