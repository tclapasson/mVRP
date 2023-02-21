
import pyodbc
import pandas as pd
from selenium import webdriver
import time


cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=hel-sql02\interal_dev;"
                      "Database=GHinfra;"
                      "Trusted_Connection=yes;")

#cursor = cnxn.cursor()

#equip = cursor.execute('SELECT [ID_EQUIPMENT],[NO_EQUIPMENT],[ID_PLANT],[GPS_LONGITUDE],[GPS_LATITUDE] FROM [GHinfra].[dbo].[EQUP_EQUIPMENT] where F_active = 1 and F_SUITE = 1')

#config = cursor.execute('SELECT [ID_SIMO_PLANT_TOUR_CONFIGURATION],[ID_PLANT],[QUESTION],[ID_EQUIPMENT],[FREQUENCE],[OCCURENCE],[ID_EQUIPMENT_LINK] FROM [GHinfra].[dbo].[USER_SIMO_PLANT_TOUR_CONFIGURATION] where F_ACTIVE = 1')

#plant = cursor.execute('SELECT [ID_PLANT],[NAME],[ADDRESS1],[ADDRESS2],[CITY],[STATE],[COUNTRY],[ZIPCODE],[PHONE1],[PHONE2],[FAX],[F_ACTIVE],[DESCRIPTION],[CONTACT_NAME],[ID_LANGUAGE] FROM [GHinfra].[dbo].[PLNT_PLANT] where NAME not like {} and  LEN(NAME) = 7 and F_ACTIVE = 1'.format("'%RIP%'"))

sql = 'SELECT [ID_PLANT],[NAME],[ADDRESS1],[ADDRESS2],[CITY],[STATE],[COUNTRY],[ZIPCODE],[PHONE1],[PHONE2],[FAX],[F_ACTIVE],[DESCRIPTION],[CONTACT_NAME],[ID_LANGUAGE] FROM [GHinfra].[dbo].[PLNT_PLANT] where NAME not like {} and  LEN(NAME) = 7 and F_ACTIVE = 1'.format("'%RIP%'")

df = pd.read_sql_query(sql, con = cnxn)

df['ADDRESS'] = df['ADDRESS1'] +" "+ df['CITY']  +" "+ df['STATE']+" "+ df['ZIPCODE'] +" "+ "QC"

url = 'https://www.google.com/maps/place/{}'.format(df['ADDRESS'][0])
url = url.replace(" ","+")
url = url.replace(",","")
url = url.replace("é","e")

lat = []
lon = []

for index, row in df.iterrows():
    driver = webdriver.Edge()
    if row['ADDRESS1'] is None:
        lat.append(None)
        lon.append(None)
        print(index, "/ 380 : ", "Pas d'adresse"," ==> ", row['DESCRIPTION'], " : Gestionnaire = ", row['CONTACT_NAME'])
    else:
        url = 'https://www.google.com/maps/place/{}'.format(row['ADDRESS'])
        url = url.replace(" ", "+")
        url = url.replace(",", "")

        driver.get(url)
        time.sleep(4)
        url = driver.current_url
        driver.close()

        coord = url.split("@")[1][:22].split(",")
        lat.append(coord[0])
        lon.append(coord[1])
        print(index, "/ 380 : ", coord[0], " - ", coord[1]," ==> ", row['DESCRIPTION'], " : Gestionnaire = ", row['CONTACT_NAME'] )

df.insert(16,"Latitude",lat)
df.insert(17,"Longitude",lon)

df.to_csv("C:/Users/tclapasson/Desktop/mVRP/PLANT_WTH_GPS.csv", sep=';', encoding='utf-8')

cnxn.close()

