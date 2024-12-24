
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
1. Arichissement v :(100.000 --> 50.000)
2. Elargissement h : Ajout de champs (rapport journalier(+ de champs) + Region)
3. Passer de SQLite à  Mysql/Postgres
4. Passer en fonctions (Tests unitaires )
5. Versionning
6. Power BI (collect (csv, DBs, Scraping, API) -> Nettoyage -> DataViz) + Python (Pandas: )

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
    nom = nom.lower().replace(" ", "-").replace("'","-").replace("œ", "oe")
    nom = unicodedata.normalize('NFD', nom)  # NFD Décomposition canonique
    nom = ''.join(c for c in nom if unicodedata.category(c) != 'Mn')
    return nom

def get_data(soup,id_ville):
    # tilisation des sélecteurs CSS pour extraire les données
    date_de_mj = soup.select_one(".topweather .updattw").text.strip().replace("a","à").replace("las","")
    temperature = soup.select_one(".topweather .icotemp").text.strip().split("°")[0]
    conditions_meteo = soup.select_one(".topweather .icotemp img").attrs["title"]
    
    vent = soup.select_one(".topweather .cdatl1 .i3 + td").text.split(" ")[0]
    
    humidite = soup.select_one(".topweather .cdatl2 .i4 + td")
    if humidite:
        humidite = humidite.text.strip().split("%")[0]
    else : humidite = soup.select_one(".topweather .cdatl1 .i4 + td").text.split("%")[0]

    pression_atmospherique = soup.select_one(".topweather .cdatl1 .i8 + td")
    if pression_atmospherique:
        pression_atmospherique = pression_atmospherique.text.split(" ")[0]
    else : pression_atmospherique = None



 
    for i in range(1,8):
                div = soup.select_one(f"div.dn{i}")
                jour = div.find("span").contents[0]
                element = div.select("i")
                #print(div.text.strip() , end=" ")
                date = element[0].text
                temperature_min = element[2].text.split("°")[0]
                temperature_max = element[1].text.split("°")[0]
                journaux.append({"date_de_mj": f"{date_de_mj}", "temperature_actuel" : f"{temperature}",
                                 "conditions_meteo" : f"{conditions_meteo}",
                                 "max" : f"{temperature_max}","min" : f"{temperature_min}",
                                 "vent" : f"{vent}",
                                 "humidite" : f"{humidite}",
                                 "pression_atmospherique" : f"{pression_atmospherique}",
                                 "jour": f"{jour}", "date" : f"{date}", 
                                 "id_ville" : f"{id_ville}"})
    return True
 
def scrape_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        response.encoding = 'utf-8'
        return BeautifulSoup(response.text, 'html.parser')
    return None

# Appel de l'API pour récupérer la liste des ville
url_communes = "https://geo.api.gouv.fr/communes"
response = requests.get(url_communes)
grandes_villes = []
if response.status_code == 200:
    villes = response.json()
    for ville in villes:
        for cle in ville.keys():
            if cle == "population":
                if (ville["population"] > 50000 ):
                    ville["nom"] = nettoyage(ville["nom"])
                    grandes_villes.append(ville)
else:
    print(f"Erreur : {response.status_code}")

url_regions = "https://geo.api.gouv.fr/regions"
response = requests.get(url_regions)
regions_fr = []
if response.status_code == 200:
    regions = response.json()
    for region in regions:
        region["nom"] = nettoyage(region["nom"])
        regions_fr.append(region)
else:
    print(f"Erreur : {response.status_code}")


connexion = sqlite3.connect("meteoDB.db")
connexion.execute("CREATE TABLE IF NOT EXISTS ville(id INTEGER PRIMARY KEY AUTOINCREMENT, ville TEXT not null, code_region TEXT, FOREIGN KEY (code_region) REFERENCES region(code) );")
connexion.execute("CREATE TABLE IF NOT EXISTS rapport_journalier(id INTEGER PRIMARY KEY AUTOINCREMENT, date_de_mj TEXT, temperature_actuel INTEGER , temp_max INTEGER, temp_min INTEGER,conditions_meteo TEXT,vent INTEGER, humidite INTEGER , pression_atmospherique INTEGER,date TEXT,jour TEXT, id_ville INTEGER, FOREIGN KEY (id_ville) REFERENCES ville(id));")
connexion.execute("CREATE TABLE IF NOT EXISTS region (code TEXT PRIMARY KEY, region TEXT);")

for region in regions:
    connexion.execute(f"INSERT INTO region (code, region) VALUES (?, ?);", (region['code'],region['nom']))
connexion.commit()

for v in grandes_villes:
    connexion.execute(f"INSERT INTO ville(ville,code_region) VALUES(?,?);", (v['nom'],v['codeRegion']))
connexion.commit()

journaux = []

regions_anciennes = [
    "alsace", "aquitaine", "auvergne", "basse-normandie", "bourgogne", "centre", "champagne-ardenne", "corse",
    "franche-comte", "haute-normandie", "ile-de-france", "languedoc-roussillon", "limousin", "lorraine",
    "midi-pyrenees", "nord-pas-de-calais", "pays-de-la-loire", "picardie", "poitou-charentes", "paca", "region-bretagne",
    "rhone-alpes", "reunion"
]

regions_mapping = [
    {"grand-est": ["alsace", "lorraine", "champagne-ardenne"]},
    {"nouvelle-aquitaine": ["aquitaine", "limousin", "poitou-charentes"]},
    {"auvergne-rhone-alpes": ["auvergne", "rhone-alpes"]},
    {"normandie": ["basse-normandie", "haute-normandie"]},
    {"bourgogne-franche-comte": ["bourgogne", "franche-comte"]},
    {"centre-val-de-loire": ["centre"]},
    {"corse": ["corse"]},
    {"ile-de-france": ["ile-de-france"]},
    {"occitanie": ["languedoc-roussillon", "midi-pyrenees"]},
    {"hauts-de-france": ["nord-pas-de-calais", "picardie"]},
    {"pays-de-la-loire": ["pays-de-la-loire"]},
    {"provence-alpes-cote-d-azur": ["provence-alpes-cote-d-azur"]},
    {"bretagne": ["region-bretagne"]},
    {"guadeloupe": ["guadeloupe"]},
    {"martinique": ["martinique"]},
    {"guyane": ["guyane"]},
    {"la-reunion": ["la-reunion"]},
    {"mayotte": ["mayotte"]},
]

for ville in grandes_villes:
    ville_name= ville["nom"]
    code_region = ville["codeRegion"]
    info_ville = connexion.execute(f"SELECT v.id, r.region FROM ville v INNER JOIN region r ON v.code_region = r.code WHERE v.ville = ? AND v.code_region = ?;", (ville_name, code_region))
    for row in info_ville:
        id_ville = row[0]
        nom_region = row[1]
    url = f"https://fr.tutiempo.net/{ville_name}.html"
    soup = scrape_data(url)
    if soup:
        region = nettoyage(soup.select_one("body > div.allcont > div.contpage.eltiempo > div:nth-child(1) > p > a").text)
        if (nom_region in region or region in nom_region) or region in regions_anciennes:
            get_data(soup,id_ville)            
        else:
            info_ville = connexion.execute(f"SELECT v.id , v.ville, r.region from ville v JOIN region r ON r.code = v.code_region WHERE v.ville = ? AND r.region = ?;", (ville_name,nom_region))
            for row in info_ville:
                id_ville = row[0]
                ville_name = row[1]
                nom_region = row[2]
                #print(id_ville,ville_name,nom_region)       
            url_2 = f"https://fr.tutiempo.net/{nom_region}/{ville_name}.html"
            soup = scrape_data(url_2)
            if soup:
                get_data(soup,id_ville)
            else:
                for r in regions_anciennes:
                    if r in nom_region or nom_region in r:
                        url_2 = f"https://fr.tutiempo.net/{r}/{ville_name}.html"
                        soup = scrape_data(url_2)
                        if soup:
                            get_data(soup,id_ville)     
                        else: 
                            print(f"Echec de la requette. code HTTP !!! : {response.status_code} ")
                            print(url_2)                                                
    else:
        print(f"Echec de la requette. code HTTP  : {response.status_code} ")
        print(url)
        
# Alimenter la table rapport_journalier
for journal in journaux:
    connexion.execute(f"INSERT INTO rapport_journalier(date_de_mj, temperature_actuel, temp_max, temp_min,conditions_meteo,vent,'humidite', 'pression_atmospherique', date, jour, id_ville) VALUES (?, ?, ?, ?, ?, ? , ?, ?, ?, ?, ?);", (journal['date_de_mj'], journal['temperature_actuel'], journal['max'], journal['min'], journal['conditions_meteo'], journal['vent'], journal['humidite'],journal['pression_atmospherique'], journal['date'] , journal['jour'] , journal['id_ville']))
    connexion.commit()
# Ecrire dans le fichier csv les données   
with open('donnees_meteo.csv', 'w', newline='') as file:
    csv_writer = csv.writer (file, delimiter=',', lineterminator='\r\n') #\r retour à la ligne, \n nouvelle ligne
    # ecrire dans le fichier csv
    csv_writer.writerow(['Id','Date mise à jour','temperature_actuel', 'Date', 'Jour','Température Max', 'Température Min', 'conditions_meteo', 'vent', 'humidite','pression_atmospherique', 'Ville', 'Region'])
    cursor = connexion.execute(f"SELECT j.id, j.date_de_mj,j.temperature_actuel, j.date, j.jour, j.temp_max, j.temp_min, j.conditions_meteo, j.vent,j.humidite, j.pression_atmospherique, v.ville, r.region FROM rapport_journalier j JOIN ville v on j.id_ville=v.id  JOIN region r ON r.code = v.code_region;")
    for ligne in cursor:
        csv_writer.writerow(ligne)
    file.close()





