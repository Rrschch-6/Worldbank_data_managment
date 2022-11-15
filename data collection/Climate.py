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
1-Get CO2 Emissions
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/EN.ATM.CO2E.PC?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/EN.ATM.CO2E.PC?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "CO2_emissions_metric_tons_per_capita": [response[1][indicator]["value"]]}
        df = pd.concat([df, pd.DataFrame.from_records(dict)])
#c.execute('CREATE TABLE IF NOT EXISTS CO2_emissions (Country_Name,Country_Code,date,CO2_emissions_metric_tons_per_capita)')
#conn.commit()
conn.execute("CREATE TABLE IF NOT EXISTS CO2_emissions (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),CO2_emissions_metric_tons_per_capita FLOAT)")

print(df.shape)
df.to_sql('CO2_emissions', conn, if_exists='replace', index = False)

"""
2-Get PM2.5 air pollution
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/EN.ATM.PM25.MC.M3?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/EN.ATM.PM25.MC.M3?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "air_pollution": [response[1][indicator]["value"]]}
        df1 = pd.concat([df1, pd.DataFrame.from_records(dict)])

#c.execute('CREATE TABLE IF NOT EXISTS air_pollution (Country_Name,Country_Code,date,air_pollution)')
#conn.commit()
conn.execute("CREATE TABLE IF NOT EXISTS air_pollution (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),air_pollution FLOAT)")

print(df1.shape)

df1.to_sql('air_pollution', conn, if_exists='replace', index = False)

"""
3-Get Average precipitation in depth (mm per year)
"""
url=f"http://api.worldbank.org/v2/country/all/indicator/AG.LND.PRCP.MM?format=json"
payload = {}
response = requests.request("GET", url, data=payload)
response=json.loads(response.text)
pages=response[0]['pages']
for page in range(1,pages+1):
    url = f"http://api.worldbank.org/v2/country/all/indicator/AG.LND.PRCP.MM?page={page}&format=json"
    response = requests.request("GET", url, data=payload)
    response = json.loads(response.text)
    for indicator in range(0,len(response[1])):
        dict={"Country Name":[response[1][indicator]["country"]["value"]],
                "Country Code":[response[1][indicator]["countryiso3code"]],
                "date": [response[1][indicator]["date"]],
                "Average_precipitation": [response[1][indicator]["value"]]}
        df2 = pd.concat([df2, pd.DataFrame.from_records(dict)])

#c.execute('CREATE TABLE IF NOT EXISTS precipitation (Country_Name,Country_Code,date,Average_precipitation)')
#conn.commit()
conn.execute("CREATE TABLE IF NOT EXISTS precipitation (Country_Name VARCHAR(255),Country_Code VARCHAR(255),date VARCHAR(255),Average_precipitation FLOAT)")

print(df2.shape)

df2.to_sql('precipitation', conn, if_exists='replace', index = False)



