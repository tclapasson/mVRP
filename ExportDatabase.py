import geopandas as gpd
import pandas as pd
import pyodbc


class ExportSQLServer:

    def __init__(self):

        self.GHinfra = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                      "Server=hel-sql02\interal_prod;"
                                      "Database=Helios_360;"
                                      "Trusted_Connection=yes;")
        self.createSQL = """                                     SET NOCOUNT ON
                                    SET ANSI_WARNINGS OFF
                                    DROP TABLE IF EXISTS [dbo].[GEOM_ITINERAIRES]
                                    CREATE TABLE [dbo].[GEOM_ITINERAIRES]
                                    (
                                        [DIST_KM] [float] NULL,
                                        [DURATION_H] [float] NULL,
                                        [TempsTrajet] [float] NULL,
                                        [TempsIntervention] [float] NULL,
                                        [TempsTotal] [float] NULL,
                                        [Contact] [varchar](max) NULL,
                                        [Date] [varchar](max) NULL,
                                        [layer] [varchar](max) NULL,
                                        [Geom] [varchar](max) NULL
                                    )"""


        self.createSQL =  """ /****** Script for SelectTopNRows command from SSMS  ******/
                            CREATE TABLE [Helios_360].[dbo].[GEOM_POINTS]
                            (
                                  [FROM_ID] [bigint] NULL
                                  ,[TO_ID] [bigint] NULL
                                  ,[DURATION_H] [bigint] NULL
                                  ,[DIST_KM] [bigint] NULL
                                  ,[ID] [bigint] NULL
                                  ,[Temps] [float] NULL
                                  ,[ID_EQUIPMENT] varchar(MAX) NULL
                                  ,[NO_EQUIPMENT] varchar(MAX) NULL
                                  ,[ID_PLANT] varchar(MAX) NULL
                                  ,[GPS_LONGITUDE] [float] NULL
                                  ,[GPS_LATITUDE] [float] NULL
                                  ,[ID_SIMO_PLANT_TOUR_CONFIGURATIONS] varchar(MAX) NULL
                                  ,[FREQUENCE] varchar(MAX) NULL
                                  ,[OCCURENCE] varchar(MAX) NULL
                                  ,[CITY] varchar(MAX) NULL
                                  ,[DESCRIPTION] varchar(MAX) NULL
                                  ,[CONTACT_NAME] varchar(MAX) NULL
                                  ,[NbVehicle] [float] NULL
                                  ,[Id_1] [float] NULL
                                  ,[F_ACTIVE] [float] NULL
                                  ,[DURATION_M_TOT] [bigint] NULL
                                  ,[TachesPLANT_TOUR] varchar(MAX) NULL
                                  ,[TachesWORK_ORDER] varchar(MAX) NULL
                                  ,[x_label] varchar(MAX) NULL
                                  ,[y_label] varchar(MAX) NULL
                                  ,[Contact] varchar(MAX) NULL
                                  ,[Date] varchar(MAX) NULL
                                  ,[layer] varchar(MAX) NULL
                                  ,[path] varchar(MAX) NULL
                                  ,[Order] [bigint] NULL
                                  ,[Itineraire] varchar(MAX) NULL
                                  )
                             """


        self.createSQL =  """CREATE TABLE [GHinfra].[dbo].[VEHICLE_START]
                        (
                            [NO_EQUIPMENT] [varchar](max) NULL,
							[CONTACT_NAME] [varchar](max) NULL,
							[GPS_LONGITUDE] [float] NULL,
							[GPS_LATITUDE] [float] NULL,
							[Lundi] [bigint] NULL,
							[Mardi] [bigint] NULL,
							[Mercredi] [bigint] NULL,
							[Jeudi] [bigint] NULL,
							[Vendredi] [bigint] NULL,
							[Samedi] [bigint] NULL,
							[Dimanche] [bigint] NULL,
							[ID] [bigint] NULL,
							[F_ACTIVE] [bigint] NULL
                        )"""
        self.cur = self.GHinfra.cursor()

    def delete(self, date, gestionnaire, tablename):
        self.delete = """   DELETE FROM [dbo].[{}]
                                    WHERE [Contact] = '{}' AND [Date] = '{}' """.format(tablename,gestionnaire, date)
        self.cur.execute(self.delete)
        self.GHinfra.commit()

    def export(self, geojson, tablename):

        with open(geojson) as f:
            contents = f.readlines()
            json = ' '.join([str(elem) for elem in contents])
        if tablename == "GEOM_ITINERAIRES":
            self.adding = """ 
                                        DECLARE @GeoJSON nvarchar(max) = N'{}';
                                        Insert Into [dbo].[{}] ([DIST_KM], [DURATION_H], [TempsTrajet], [TempsIntervention],[TempsTotal], [Contact], [Date] ,[layer] ,[Geom], [Itineraire])
                                        SELECT *
                                        FROM 
                                        OPENJSON(@GeoJSON, '$.features')
                                                WITH (
                                                        [DIST_KM] nvarchar(300) '$.properties.DIST_KM' ,
                                                        [DURATION_H] nvarchar(300) '$.properties.DURATION_H' ,
                                                        [TempsTrajet] nvarchar(300) '$.properties.TempsTrajet' ,
                                                        [TempsIntervention] nvarchar(300) '$.properties.TempsIntervention',
                                                        [TempsTotal] nvarchar(300) '$.properties.TempsTotal' ,
                                                        [Contact] nvarchar(300) '$.properties.Contact' ,
                                                        [Date] nvarchar(300) '$.properties.Date',
                                                        [layer] nvarchar(300) '$.properties.layer' ,
                                                        [Geom] nvarchar(max) '$.properties.Geom',
                                                        [Itineraire] nvarchar(max) '$.properties.Itineraire'
                                                    ) """.format(json, tablename)

            self.cur.execute(self.adding)
            self.GHinfra.commit()
        elif tablename == "GEOM_POINTS":
            self.adding = """ 
                                        DECLARE @GeoJSON nvarchar(max) = N'{}';
                                        Insert Into [dbo].[{}] ([FROM_ID],[TO_ID],[DURATION_H],[DIST_KM],[ID],[Temps],[ID_EQUIPMENT],[NO_EQUIPMENT],[ID_PLANT],[GPS_LONGITUDE],[GPS_LATITUDE],[ID_SIMO_PLANT_TOUR_CONFIGURATIONS],[FREQUENCE],[OCCURENCE],[CITY],[DESCRIPTION],[CONTACT_NAME],[NbVehicle],[Id_1],[F_ACTIVE],[DURATION_M_TOT],[TachesPLANT_TOUR],[TachesWORK_ORDER],[x_label],[y_label],[Contact],[Date],[layer],[path], [Order], [Itineraire])
                                        SELECT *
                                        FROM 
                                        OPENJSON(@GeoJSON, '$.features')
                                                WITH (
                                                      [FROM_ID] nvarchar(300) '$.properties.FROM_ID'
                                                      ,[TO_ID] nvarchar(300) '$.properties.TO_ID'
                                                      ,[DURATION_H] nvarchar(300) '$.properties.DURATION_H'
                                                      ,[DIST_KM] nvarchar(300) '$.properties.DIST_KM'
                                                      ,[ID] nvarchar(300) '$.properties.ID'
                                                      ,[Temps] nvarchar(300) '$.properties.Temps'
                                                      ,[ID_EQUIPMENT] nvarchar(300) '$.properties.ID_EQUIPMENT'
                                                      ,[NO_EQUIPMENT] nvarchar(300) '$.properties.NO_EQUIPMENT'
                                                      ,[ID_PLANT] nvarchar(300) '$.properties.ID_PLANT'
                                                      ,[GPS_LONGITUDE] nvarchar(300) '$.properties.GPS_LONGITUDE'
                                                      ,[GPS_LATITUDE] nvarchar(300) '$.properties.GPS_LATITUDE'
                                                      ,[ID_SIMO_PLANT_TOUR_CONFIGURATIONS] nvarchar(300) '$.properties.ID_SIMO_PLANT_TOUR_CONFIGURATIONS'
                                                      ,[FREQUENCE] nvarchar(300) '$.properties.FREQUENCE'
                                                      ,[OCCURENCE] nvarchar(300) '$.properties.OCCURENCE'
                                                      ,[CITY] nvarchar(300) '$.properties.CITY'
                                                      ,[DESCRIPTION] nvarchar(300) '$.properties.DESCRIPTIO'
                                                      ,[CONTACT_NAME] nvarchar(300) '$.properties.CONTACT_NAME'
                                                      ,[NbVehicle] nvarchar(300) '$.properties.NbVehicle'
                                                      ,[Id_1] nvarchar(300) '$.properties.Id_1'
                                                      ,[F_ACTIVE] nvarchar(300) '$.properties.F_ACTIVE'
                                                      ,[DURATION_M_TOT] nvarchar(300) '$.properties.DURATION_M_TOT'
                                                      ,[TachesPLANT_TOUR] nvarchar(max) '$.properties.TachesPLANT_TOUR'
                                                      ,[TachesWORK_ORDER] nvarchar(max) '$.properties.TachesWORK_ORDER'
                                                      ,[x_label] nvarchar(300) '$.properties.x_label'
                                                      ,[y_label] nvarchar(300) '$.properties.y_label'
                                                      ,[Contact] nvarchar(300) '$.properties.Contact'
                                                      ,[Date] nvarchar(300) '$.properties.Date'
                                                      ,[layer] nvarchar(300) '$.properties.layer'
                                                      ,[path] nvarchar(300) '$.properties.path'
                                                      ,[Ordre] nvarchar(300) '$.properties.Ordre'
                                                      ,[Itineraire] nvarchar(300) '$.properties.Itineraire'
                                                    ) """.format(json, tablename)

            self.cur.execute(self.adding)
            self.GHinfra.commit()
