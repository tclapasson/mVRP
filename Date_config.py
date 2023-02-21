import datetime
import os.path
import os

import pandas as pd
import pyodbc
from selenium import webdriver
import time
import duckdb

class DateConfig:

    def __init__(self, Y, M, D, Gestionnaires, Pointscible = 3):
        self.Gestionnaires = Gestionnaires
        if len(self.Gestionnaires) > 1:
            self.GestionnairesSQL = tuple(self.Gestionnaires)
        else :
            self.GestionnairesSQL = str(tuple(self.Gestionnaires)).replace(',','')
        self.Y = Y
        self.M = M
        self.D = D
        self.Pointscible = Pointscible
        self.date = datetime.date(int(self.Y), int(self.M), int(self.D))
        self.list = []
        self.listGest = []
        self.GHinfra = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                      "Server=hel-sql02\interal_prod;"
                                      "Database=GHinfra;"
                                      "Trusted_Connection=yes;")
        self.sql1 = """with A as (
                            SELECT 0.3 as TIME_INTER, 
                            E.[ID_EQUIPMENT],
                            [NO_EQUIPMENT],
                            C.[ID_PLANT] as [ID_PLANT],
                            [GPS_LONGITUDE],
                            [GPS_LATITUDE],
                            [ID_SIMO_PLANT_TOUR_CONFIGURATION],
                            [FREQUENCE],
                            [OCCURENCE] 
                            FROM [GHinfra].[dbo].[EQUP_EQUIPMENT] E join [GHinfra].[dbo].[USER_SIMO_PLANT_TOUR_CONFIGURATION] C on E.ID_EQUIPMENT = C.ID_EQUIPMENT 
                            where E.F_active = 1 and C.F_active = 1 and C.FREQUENCE not like '%0%' and C.OCCURENCE is not null and (C.CATEGORY1 is not null or C.CATEGORY2 is not null or C.CATEGORY3 is not null)),
                            B as (
                            SELECT  SUM(TIME_INTER) as Temps,
                                    [ID_EQUIPMENT],
                                    [NO_EQUIPMENT],
                                    [ID_PLANT],
                                    [GPS_LONGITUDE],
                                    [GPS_LATITUDE],
                                    STRING_AGG([ID_SIMO_PLANT_TOUR_CONFIGURATION], ', ' )  as TachesPLANT_TOUR,
                                    [FREQUENCE],
                                    [OCCURENCE] 
                                    FROM A GROUP BY [NO_EQUIPMENT],[ID_EQUIPMENT],[ID_PLANT],[GPS_LONGITUDE],[GPS_LATITUDE],[FREQUENCE],[OCCURENCE]), 
                            C as (
                            SELECT  [ID_PLANT] as plnt,
                                    [CITY],[DESCRIPTION],
                                    [CONTACT_NAME] 
                                    FROM [GHinfra].[dbo].[PLNT_PLANT] 
                                    where  NAME not like '%RIP%' and LEN(NAME) = 7 and F_ACTIVE = 1)
							SELECT	Temps, 
									ID_EQUIPMENT, 
									NO_EQUIPMENT, 
									ID_PLANT, 
									GPS_LATITUDE, 
									GPS_LONGITUDE, 
									TachesPLANT_TOUR, 
									FREQUENCE, 
									OCCURENCE, 
									CITY, 
									C.[DESCRIPTION] as [DESCRIPTION],
									CONTACT_NAME 
									FROM B RIGHT JOIN C on B.ID_PLANT = C.plnt
									where GPS_LATITUDE is not null"""
        self.TourConfig = pd.read_sql_query(self.sql1, con=self.GHinfra)

        self.sql2 = """ WITH A as (
                                    SELECT [ID_WORK_ORDER_HEADER] as TachesWORK_ORDER
                                          ,[ID_EQUIPMENT]
                                          ,[DATE_OPEN]
                                          ,[DATE_START]
                                          ,[DESCRIPTION]
                                          ,[TOTAL_TIME_PLAN] as Temps
                                          ,ID_WO_STATUS
                                      FROM [GHinfra].[dbo].[MAINT_WORK_ORDER_HEADER]
                                      WHERE DATE_START = '{}' AND ID_WO_STATUS NOT IN ('4','8','5') or [DATE_OPEN] = '{}' AND DATE_START = null AND ID_WO_STATUS NOT IN ('4','8','5')),
                                B as (
                                    SELECT [ID_PLANT] as PLT,[CITY],[DESCRIPTION],[CONTACT_NAME] 
                                    FROM [GHinfra].[dbo].[PLNT_PLANT] 
                                    WHERE NAME not like '%RIP%' and  LEN(NAME) = 7 and F_ACTIVE = 1),
                                C as (	   
                                    SELECT [ID_EQUIPMENT] as eqip
                                          ,[NO_EQUIPMENT]
                                          ,[GPS_LONGITUDE]
                                          ,[GPS_LATITUDE]
                                          ,[ID_PLANT]
                                    FROM [GHinfra].[dbo].[EQUP_EQUIPMENT]
                                    WHERE F_active = 1)
                                SELECT TachesWORK_ORDER, ID_EQUIPMENT, B.[DESCRIPTION], Temps, NO_EQUIPMENT, GPS_LATITUDE, GPS_LONGITUDE, ID_PLANT, CITY, CONTACT_NAME
                                FROM A JOIN C on A.ID_EQUIPMENT = C.eqip JOIN B on C.ID_PLANT = B.PLT 
                                where CONTACT_NAME IN {} """.format(self.date,self.date,self.GestionnairesSQL)

        self.WorkOrder = pd.read_sql_query(self.sql2, con=self.GHinfra)

        self.sql3 = """SELECT [NO_EQUIPMENT]
                    ,[CONTACT_NAME]
                    ,[GPS_LONGITUDE]
                    ,[GPS_LATITUDE]
                    ,[Lundi]
                    ,[Mardi]
                    ,[Mercredi]
                    ,[Jeudi]
                    ,[Vendredi]
                    ,[Samedi]
                    ,[Dimanche]
                    ,[ID]
                    ,[F_ACTIVE] 
                    FROM [Helios_360].[dbo].[GEOM_VEHICLE_START]
                    WHERE [CONTACT_NAME] IN {}""".format(self.GestionnairesSQL)
        self.VehicleStart = pd.read_sql_query(self.sql3, con=self.GHinfra)

        self.csvDep = os.getcwd() + "/startGestion.csv"
        self.Departs = pd.read_csv(self.csvDep, sep=";")
        self.exitdir = (os.getcwd() + "/Sortie/" + "{}-{}-{}/{}".format(self.Y, self.M, self.D, ';'.join(self.Gestionnaires)))
        self.csvOUT = self.exitdir + "/{}-{}-{}-{}.csv".format(';'.join(self.Gestionnaires), self.Y, self.M, self.D)


    def plantTourConfig(self):
        for index, row in self.TourConfig.iterrows():
            if row['OCCURENCE'] != "" and row['OCCURENCE'] != None:
                if row['FREQUENCE'] == 1:
                    occurences = list(map(int, list(map(lambda x: x.replace("1", "8"), row['OCCURENCE'].split(",")))))
                    if self.date.weekday() + 2 in occurences:
                        self.list.append(index)
                elif row['FREQUENCE'] == 2:
                    occurences = list(map(int, row['OCCURENCE'].split(",")))
                    if self.date.day in occurences:
                        self.list.append(index)
                elif row['FREQUENCE'] == 3:
                    occurences = list(map(int, row['OCCURENCE'].split(",")))
                    if int(self.date.strftime("%j")) in occurences:
                        self.list.append(index)
        return self.TourConfig.iloc[self.list]

    def gestinnaireTourConfig(self):
        df = DateConfig(Y=self.Y, M=self.M, D=self.D, Gestionnaires=self.Gestionnaires).plantTourConfig()
        self.listGest = []
        for Gestionnaire in self.Gestionnaires:
            for index, row in df.iterrows():
                if row['CONTACT_NAME'] == Gestionnaire:
                    self.listGest.append(index)
        return df.loc[self.listGest]

    def cleanTourConfig(self):
        df = DateConfig(Y=self.Y, M=self.M, D=self.D, Gestionnaires=self.Gestionnaires).gestinnaireTourConfig()
        dfSQL = duckdb.query("""  WITH A as ( SELECT 
                                    ID_EQUIPMENT,
                                    NO_EQUIPMENT,
                                    ID_PLANT,
                                    string_agg(FREQUENCE, ' - ' )  as FREQUENCE, 
                                    string_agg(OCCURENCE, ' - ' )  as OCCURENCE,
                                    string_agg(TachesPLANT_TOUR, ' - ' )  as TachesPLANT_TOUR, 
                                    SUM(Temps) as Temps,
                                    GPS_LONGITUDE,
                                    GPS_LATITUDE,
                                    CONTACT_NAME,
                                    ID_PLANT,CITY,
                                    DESCRIPTION 
                                    FROM df 
                                    GROUP BY ID_EQUIPMENT, NO_EQUIPMENT ,ID_PLANT, GPS_LONGITUDE,GPS_LATITUDE, CONTACT_NAME, CITY,ID_PLANT,DESCRIPTION)
                                    
                                    SELECT 
                                            string_agg(ID_EQUIPMENT, ' / ' )  as ID_EQUIPMENT, 
                                            string_agg(NO_EQUIPMENT, ' / ' )  as NO_EQUIPMENT, 
                                            ID_PLANT, 
                                            string_agg(TachesPLANT_TOUR, ' - ' )  as TachesPLANT_TOUR, 
                                            string_agg(FREQUENCE, ' - ' )  as FREQUENCE, 
                                            string_agg(OCCURENCE, ' - ' )  as OCCURENCE,
                                            SUM(Temps) as Temps,
                                            GPS_LONGITUDE,
                                            GPS_LATITUDE,
                                            CONTACT_NAME,
                                            ID_PLANT,CITY,
                                            DESCRIPTION 
                                            FROM A
                                            GROUP BY GPS_LONGITUDE,GPS_LATITUDE, CONTACT_NAME,CITY,ID_PLANT,DESCRIPTION""").df()


        return dfSQL

    def cleanWorkOrder(self):
        df = self.WorkOrder
        dfSQL = duckdb.query(""" SELECT *                                                                                                                                         
                                    FROM df """).df()
        return df.astype(object)


    def departs(self):
        jourcorrespond = {0: 'Lundi', 1: 'Mardi', 2: 'Mercredi', 3: 'Jeudi', 4: 'Vendredi', 5: 'Samedi', 6: 'Dimanche'}
        jouractuel = self.date.weekday()
        self.VehicleStart = self.VehicleStart.rename({jourcorrespond[jouractuel]: 'NbVehicle'}, axis=1)  # new method
        for index, col in jourcorrespond.items():
            if jourcorrespond[index] in self.VehicleStart:
                self.VehicleStart.drop(jourcorrespond[index], inplace=True, axis=1)
        self.listGest = []
        for index, row in self.VehicleStart.iterrows():
            if row['F_ACTIVE'] == 1:
                self.listGest.append(index)
        self.VehicleStart = self.VehicleStart.loc[self.listGest]
        self.listGest = []
        for Gestionnaire in self.Gestionnaires:
            for index, row in self.VehicleStart.iterrows():
                if row['CONTACT_NAME'] == Gestionnaire:
                    self.listGest.append(index)
        return self.VehicleStart.loc[self.listGest]

    def exitDir(self, df):
        if len(df) > 60:
            max = 59
        else:
            max = len(df)
        if not os.path.exists(self.exitdir):
            os.makedirs(self.exitdir)
            df.iloc[list(range(0,max)),].to_csv(self.csvOUT, sep=";", index=True, encoding="utf-8-sig")
        else:
            df.iloc[list(range(0,max)),].to_csv(self.csvOUT, sep=";", index=True, encoding="utf-8-sig")


    def datesNoNAGPStoCSV(self):
        df = DateConfig(Y=self.Y, M=self.M, D=self.D, Gestionnaires=self.Gestionnaires, Pointscible=self.Pointscible)
        if self.Pointscible == 1:
            df1 = df.cleanTourConfig()
            df1.drop(df1.index[df1.isnull().any(axis=1)], 0, inplace=True)
            df2 = df.departs()
            df3 = pd.merge(df2, df1, on=['CONTACT_NAME','NO_EQUIPMENT','GPS_LONGITUDE','GPS_LATITUDE'], how='outer')
            df.exitDir(df3)
            return self.exitdir, self.csvOUT
        if self.Pointscible == 2:
            if not self.WorkOrder.empty:
                df1 = df.cleanWorkOrder()
                df1.drop(df1.index[df1.isnull().any(axis=1)], 0, inplace=True)
                df2 = df.departs()
                df3 = pd.merge(df2, df1, on=['CONTACT_NAME','NO_EQUIPMENT','GPS_LONGITUDE','GPS_LATITUDE'], how='outer')
                df.exitDir(df3)
                return self.exitdir, self.csvOUT
            else:
                return print("Pas de bon de travail geolocalisable pour {} le {}".format(self.GestionnairesSQL, self.date))
        if self.Pointscible == 3:
            df1 = df.cleanTourConfig()
            df1.drop(df1.index[df1.isnull().any(axis=1)], 0, inplace=True)
            df2 = df.cleanWorkOrder()
            df2.drop(df2.index[df2.isnull().any(axis=1)], 0, inplace=True)
            df3 = df.departs()
            df4 = pd.merge(df3, df1, on=['CONTACT_NAME', 'NO_EQUIPMENT', 'GPS_LONGITUDE', 'GPS_LATITUDE'], how='outer')
            df5 = pd.merge(df4, df2, on=['CONTACT_NAME', 'NO_EQUIPMENT', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'DESCRIPTION', 'ID_EQUIPMENT','Temps', 'ID_PLANT', 'CITY'], how='outer')
            df.exitDir(df5)
            return self.exitdir, self.csvOUT
        else:
            return print("Indice de cas non valide, veuillez choisir entre 1 : TourConfig, 2 : WorkOrder, 3 : Tourconfig+WorkOrder")

#x = DateConfig(Y="2022", M="11", D="01", Gestionnaires=['Anick Laplante'], Pointscible=3).cleanTourConfig()
