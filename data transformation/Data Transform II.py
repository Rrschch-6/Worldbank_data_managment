import requests
import json
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import pandas as pd
import requests
import json
import os
from google.cloud import bigquery

from pandas.io import gbq
#conn = sqlite3.connect('worldbank_database')
#c = conn.cursor()
credentials_path='python_BQ.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=credentials_path
engine=create_engine("mysql+mysqldb://root:1361@35.242.242.190/worldbank_database")
conn=engine.connect()
client = bigquery.Client()
query_job_bigQuery = client.query("""DROP TABLE workbankproject-367309.worldbank_dataset.worldbank_table;""")

''' Transforming the urban rural access tables'''

df = pd.read_sql_query ('''
                               SELECT
                               *
                               FROM water_services_rural
                               ''', conn)

buffer = pd.read_sql_query ('''
                              SELECT
                              *
                              FROM water_services_urban
                              ''', conn)
df=pd.merge(df,buffer,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])
buffer = pd.read_sql_query ('''
                               SELECT
                               *
                               FROM electricity_rural
                               ''', conn)
df=pd.merge(df,buffer,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])
buffer = pd.read_sql_query ('''
                               SELECT
                               *
                               FROM electricity_urban
                               ''', conn)
df=pd.merge(df,buffer,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])

buffer = pd.read_sql_query ('''
                               SELECT
                               *
                               FROM sanitation_rural
                               ''', conn)
df=pd.merge(df,buffer,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])
buffer = pd.read_sql_query ('''
                               SELECT
                               *
                               FROM sanitation_urban
                               ''', conn)
df=pd.merge(df,buffer,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])

''' Transforming the climate tables'''
buffer = pd.read_sql_query ('''
                               SELECT
                               *
                               FROM CO2_emissions
                               ''', conn)
df=pd.merge(df,buffer,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])

buffer = pd.read_sql_query ('''
                              SELECT
                              *
                              FROM air_pollution
                              ''', conn)
df=pd.merge(df,buffer,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])
buffer = pd.read_sql_query ('''
                               SELECT
                               *
                               FROM precipitation
                               ''', conn)
df=pd.merge(df,buffer,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])


''' Transforming Environment and Agriculture '''

buffer = pd.read_sql_query ('''
                               SELECT
                               *
                               FROM Forest_area
                               ''', conn)
df=pd.merge(df,buffer,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])

buffer = pd.read_sql_query ('''
                              SELECT
                              *
                              FROM Total_natural_resources_rents
                              ''', conn)
df=pd.merge(df,buffer,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])
buffer = pd.read_sql_query ('''
                              SELECT
                              *
                              FROM Terrestrial_protected_areas
                              ''', conn)
df=pd.merge(df,buffer,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])
buffer = pd.read_sql_query ('''
                              SELECT
                              *
                              FROM Terrestrial_and_marine_protected_areas
                              ''', conn)
df=pd.merge(df,buffer,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])
buffer = pd.read_sql_query ('''
                              SELECT
                              *
                              FROM Marine_protected_areas
                              ''', conn)
df=pd.merge(df,buffer,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])
buffer = pd.read_sql_query ('''
                              SELECT
                              *
                              FROM Agricultural_land
                              ''', conn)
df=pd.merge(df,buffer,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])
df=df[df['date']>='2000']
df=df.iloc[:, [0,1,3,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17]]

buffer=pd.read_csv('ourworldindata_urban-and-rural-population.csv')
buffer=buffer[buffer['date']>=2000]
buffer=buffer.astype({'date':'str','Urban population':'float64','Rural population':'float64','Total Population':'float64'})
df=pd.merge(buffer,df,how='left',on=['Country Name','date'],suffixes=("","X")).drop(columns=['Country CodeX'])
#Formating the date
df['date']= pd.to_datetime(df['date'],format='%Y')
df.to_csv('BigQuery data.csv',index=False)

# json_rows=[]
#
# for index, row in df.iterrows():
#     message_dict = {
#         'Country_Name': row['Country Name'],
#         'Country_Code': row['Country Code'],
#         'date': str(row['date']),
#         'Urban_population': row['Urban population'],
#         'Rural_population': row['Rural population'],
#         'Total_Population': row['Total Population'],
#         'People_using_at_least_basic_drinking_water_services_rural': row['People_using_at_least_basic_drinking_water_services_rural'],
#         'People_using_at_least_basic_drinking_water_services_urban': row['People_using_at_least_basic_drinking_water_services_urban'],
#         'Access_to_electricity_rural': row['Access_to_electricity_rural'],
#         'Access_to_electricity_urban': row['Access_to_electricity_urban'],
#         'Access_to_sanitation_rural': row['Access_to_sanitation_rural'],
#         'Access_to_sanitation_urban': row['Access_to_sanitation_urban'],
#         'CO2_emissions_metric_tons_per_capita': row['CO2_emissions_metric_tons_per_capita'],
#         'air_pollution': row['air_pollution'],
#         'Average_precipitation': row['Average_precipitation'],
#         'Forest_area_percentage_of_land_area': row['Forest_area_percentage_of_land_area'],
#         'Total_natural_resources_rents': row['Total_natural_resources_rents'],
#         'Terrestrial_protected_areas_percentage_of_total_land_area': row['Terrestrial_protected_areas_percentage_of_total_land_area'],
#         'Terrestrial_and_marine_protected_areas_percentage_of_total_territorial_area': row['Terrestrial_and_marine_protected_areas_per_of_total_terr_area'],
#         'Marine_protected_areas_percentage_of_territorial_waters': row['Marine_protected_areas_percentage_of_territorial_waters'],
#         'Agricultural_land_percentage_of_land_area': row['Agricultural_land_percentage_of_land_area']
#     }
#     json_rows.append(message_dict)
# client.insert_rows_json("workbankproject-367309.worldbank_dataset.worldbank_table", json_rows)

df.rename(columns = {'Country Name':'Country_Name','Country Code':'Country_Code','Urban population':'Urban_population','Rural population':'Rural_population','Total Population':'Total_Population'}, inplace = True)

client.load_table_from_dataframe(df,"workbankproject-367309.worldbank_dataset.worldbank_table")
