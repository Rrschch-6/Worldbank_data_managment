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
1-Get Rural water data
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/SH.H2O.BASW.RU.ZS?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/SH.H2O.BASW.RU.ZS?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "People_using_at_least_basic_drinking_water_services_rural": [response[1][indicator]["value"]]}
        df = pd.concat([df, pd.DataFrame.from_records(dict)])
#c.execute("CREATE TABLE IF NOT EXISTS 'water_services_rural' (Country_Name VARCHAR(255),Country_Code,date VARCHAR(255),People_using_at_least_basic_drinking_water_services_rural FLOAT)")
#conn.commit()
conn.execute("CREATE TABLE IF NOT EXISTS water_services_rural (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),People_using_at_least_basic_drinking_water_services_rural FLOAT)")
print(df.shape)
df.to_sql('water_services_rural', conn, if_exists='replace', index = False)

"""
2-Get Urban water data
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/SH.H2O.BASW.UR.ZS?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/SH.H2O.BASW.UR.ZS?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "People_using_at_least_basic_drinking_water_services_urban": [response[1][indicator]["value"]]}
        df1 = pd.concat([df1, pd.DataFrame.from_records(dict)])

#c.execute('CREATE TABLE IF NOT EXISTS water_services_urban (Country_Name,Country_Code,date,People_using_at_least_basic_drinking_water_services_urban)')
#conn.commit()
print(df1.shape)
conn.execute("CREATE TABLE IF NOT EXISTS water_services_urban (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),People_using_at_least_basic_drinking_water_services_urban FLOAT)")

df1.to_sql('water_services_urban', conn, if_exists='replace', index = False)

"""
3-Get Rural Electricity data
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/EG.ELC.ACCS.RU.ZS?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/EG.ELC.ACCS.RU.ZS?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "Access_to_electricity_rural": [response[1][indicator]["value"]]}
        df3 = pd.concat([df3, pd.DataFrame.from_records(dict)])

#c.execute('CREATE TABLE IF NOT EXISTS electricity_rural (Country_Name,Country_Code,date,Access_to_electricity_rural)')
#conn.commit()
conn.execute("CREATE TABLE IF NOT EXISTS electricity_rural (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),Access_to_electricity_rural FLOAT)")

print(df3.shape)

df3.to_sql('electricity_rural', conn, if_exists='replace', index = False)

"""
4-Get Urban Electricity data
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/EG.ELC.ACCS.UR.ZS?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/EG.ELC.ACCS.UR.ZS?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "Access_to_electricity_urban": [response[1][indicator]["value"]]}
        df4 = pd.concat([df4, pd.DataFrame.from_records(dict)])

#c.execute('CREATE TABLE IF NOT EXISTS electricity_urban (Country_Name,Country_Code,date,Access_to_electricity_urban)')
#conn.commit()
conn.execute("CREATE TABLE IF NOT EXISTS electricity_urban (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),Access_to_electricity_urban FLOAT)")

print(df4.shape)

df4.to_sql('electricity_urban', conn, if_exists='replace', index = False)

"""
5-Get rural sanitation data
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/SH.STA.BASS.RU.ZS?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/SH.STA.BASS.RU.ZS?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "Access_to_sanitation_rural": [response[1][indicator]["value"]]}
        df5= pd.concat([df5, pd.DataFrame.from_records(dict)])

#c.execute('CREATE TABLE IF NOT EXISTS sanitation_rural (Country_Name,Country_Code,date,Access_to_sanitation_rural)')
#conn.commit()
conn.execute("CREATE TABLE IF NOT EXISTS sanitation_rural (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),Access_to_sanitation_rural FLOAT)")

print(df5.shape)

df5.to_sql('sanitation_rural', conn, if_exists='replace', index = False)

"""
6-Get urban sanitation data
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/SH.STA.BASS.UR.ZS?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/SH.STA.BASS.UR.ZS?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "Access_to_sanitation_urban": [response[1][indicator]["value"]]}
        df6= pd.concat([df6, pd.DataFrame.from_records(dict)])

#c.execute('CREATE TABLE IF NOT EXISTS sanitation_urban (Country_Name,Country_Code,date,Access_to_sanitation_urban)')
#conn.commit()
conn.execute("CREATE TABLE IF NOT EXISTS sanitation_urban (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),Access_to_sanitation_urban FLOAT)")

print(df6.shape)

df6.to_sql('sanitation_urban', conn, if_exists='replace', index = False)




