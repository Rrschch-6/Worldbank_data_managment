import requests
import json
import pandas as pd
import sqlite3
import openpyxl
import mysql.connector
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import inspect
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData
import pandas as pd
import requests
import json

#conn = sqlite3.connect('worldbank_database')
#c = conn.cursor()
engine=create_engine("mysql+mysqldb://root:1361@35.242.242.190/worldbank_database")
conn=engine.connect()

df=pd.DataFrame()
df1=pd.DataFrame()
df2=pd.DataFrame()
df3=pd.DataFrame()
df4=pd.DataFrame()
df5=pd.DataFrame()
df6=pd.DataFrame()

"""
1-Get Forest Area
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/AG.LND.FRST.ZS?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/AG.LND.FRST.ZS?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "Forest_area_percentage_of_land_area": [response[1][indicator]["value"]]}
        df = pd.concat([df, pd.DataFrame.from_records(dict)])
#.execute('CREATE TABLE IF NOT EXISTS Forest_area (Country_Name,Country_Code,date,Forest_area_percentage_of_land_area)')
#conn.commit()
conn.execute("CREATE TABLE IF NOT EXISTS Forest_area (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),Forest_area_percentage_of_land_area FLOAT)")

print(df.shape)
df.to_sql('Forest_area', conn, if_exists='replace', index = False)

"""
2-Total natural resource rents
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/NY.GDP.TOTL.RT.ZS?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/NY.GDP.TOTL.RT.ZS?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "Total_natural_resources_rents": [response[1][indicator]["value"]]}
        df1 = pd.concat([df1, pd.DataFrame.from_records(dict)])
#c.execute('CREATE TABLE IF NOT EXISTS Total_natural_resources_rents (Country_Name,Country_Code,date,Total_natural_resources_rents)')
#conn.commit()
conn.execute("CREATE TABLE IF NOT EXISTS Total_natural_resources_rents (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),Total_natural_resources_rents FLOAT)")

print(df1.shape)
df1.to_sql('Total_natural_resources_rents', conn, if_exists='replace', index = False)

"""
3-Terrestrial protected areas (% of total land area)
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/ER.LND.PTLD.ZS?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/ER.LND.PTLD.ZS?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "Terrestrial_protected_areas_percentage_of_total_land_area": [response[1][indicator]["value"]]}
        df2 = pd.concat([df2, pd.DataFrame.from_records(dict)])
#c.execute('CREATE TABLE IF NOT EXISTS Terrestrial_protected_areas (Country_Name,Country_Code,date,Terrestrial_protected_areas_percentage_of_total_land_area)')
#conn.commit()
conn.execute("CREATE TABLE IF NOT EXISTS Terrestrial_protected_areas (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),Terrestrial_protected_areas_percentage_of_total_land_area FLOAT)")

print(df2.shape)
df2.to_sql('Terrestrial_protected_areas', conn, if_exists='replace', index = False)

"""
4-Terrestrial and marine protected areas (% of total territorial area)
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/ER.PTD.TOTL.ZS?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/ER.PTD.TOTL.ZS?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "Terrestrial_and_marine_protected_areas_per_of_total_terr_area": [response[1][indicator]["value"]]}
        df3 = pd.concat([df3, pd.DataFrame.from_records(dict)])
#c.execute('CREATE TABLE IF NOT EXISTS Terrestrial_and_marine_protected_areas (Country_Name,Country_Code,Terrestrial_and_marine_protected_areas_percentage_of_total_territorial_area)')
#conn.commit()
conn.execute("CREATE TABLE IF NOT EXISTS Terrestrial_and_marine_protected_areas (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),Terrestrial_and_marine_protected_areas_per_of_total_terr_area FLOAT)")

print(df3.shape)
df3.to_sql('Terrestrial_and_marine_protected_areas', conn, if_exists='replace', index = False)

"""
5-Marine protected areas (% of territorial waters)
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/ER.MRN.PTMR.ZS?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/ER.MRN.PTMR.ZS?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "Marine_protected_areas_percentage_of_territorial_waters": [response[1][indicator]["value"]]}
        df4 = pd.concat([df4, pd.DataFrame.from_records(dict)])
#c.execute('CREATE TABLE IF NOT EXISTS Marine_protected_areas (Country_Name,Country_Code,Marine_protected_areas_percentage_of_territorial_waters)')
#conn.commit()
conn.execute("CREATE TABLE IF NOT EXISTS Marine_protected_areas (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),Marine_protected_areas_percentage_of_territorial_waters FLOAT)")

print(df4.shape)
df4.to_sql('Marine_protected_areas', conn, if_exists='replace', index = False)

"""
6-Agricultural land (% of land area)
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/AG.LND.AGRI.ZS?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/AG.LND.AGRI.ZS?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "Agricultural_land_percentage_of_land_area": [response[1][indicator]["value"]]}
        df5 = pd.concat([df5, pd.DataFrame.from_records(dict)])
#c.execute('CREATE TABLE IF NOT EXISTS Agricultural_land (Country_Name,Country_Code,Agricultural_land_percentage_of_land_area)')
#conn.commit()
conn.execute("CREATE TABLE IF NOT EXISTS Agricultural_land (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),Agricultural_land_percentage_of_land_area FLOAT)")

print(df5.shape)
df5.to_sql('Agricultural_land', conn, if_exists='replace', index = False)