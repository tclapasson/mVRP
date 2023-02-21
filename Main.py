"""
Model exported as python.
Name : Modèle
Group :
With QGIS : 32211
"""
import os.path
import sys
sys.path.append(os.getcwd())
sys.path.append('C:/OSGeo4W/apps/qgis-ltr/python')
sys.path.append('C:/OSGeo4W/apps/qgis-ltr/python/plugins')
sys.path.append('C:/Users/tclapasson/AppData/Roaming/QGIS/QGIS3/profiles/default/python/plugins/')
from qgis.gui import QgsLayerTreeMapCanvasBridge, QgsMapCanvas
from qgis.core import QgsProcessingFeedback, QgsApplication, QgsProcessingContext, QgsProcessingAlgorithm, \
    QgsVectorLayer, QgsProject, QgsProcessingParameterVectorLayer, QgsProcessing, QgsProcessingParameterField, \
    QgsProcessingParameterFeatureSource, QgsProcessingParameterEnum, QgsProcessingParameterFeatureSink, \
    QgsProcessingMultiStepFeedback, QgsExpression, QgsCoordinateReferenceSystem, QgsField, QgsFeature, QgsVectorFileWriter, QgsCoordinateTransformContext, edit
from PyQt5.QtCore import QVariant
import pandas as pd
import numpy
import datetime
from Date_config import DateConfig
from VRP import mvrp
from Planification import Planification
from modle import Modle
from solutionsIteratif import Solutions
from GroupJson import GroupJson
from ExportDatabase import ExportSQLServer

class Main:
    def __init__(self, Y, M, D, gestionnaire, pointscibles):
        self.Y = Y
        self.M = M
        self.D = D
        self.gestionnaire = gestionnaire
        self.pointscibles = pointscibles
        self.date = datetime.date(int(self.Y), int(self.M), int(self.D))

    def exe(self):
        pathout, csv = DateConfig(Y=self.Y, M=self.M, D=self.D, Gestionnaires=self.gestionnaire, Pointscible=self.pointscibles).datesNoNAGPStoCSV()
        uri = 'file:///' + csv + '?type=csv&delimiter=;&maxFields=10000&detectTypes=yes&decimalPoint=,&geomType=none&subsetIndex=no&watchFile=no'
        lyr = QgsVectorLayer(uri, 'CSV', 'delimitedtext')
        modle = Modle()
        parameters = {'CSV': lyr,
                      'X': 'GPS_LONGITUDE',
                      'Y': 'GPS_LATITUDE',
                      'Points': pathout + '/Points.geojson',
                      'Tableur': pathout + '/Tableur.xlsx'}  # add your parameters here
        modle.initAlgorithm()
        feedback = QgsProcessingFeedback()
        context = QgsProcessingContext()
        output = modle.processAlgorithm(parameters, context, feedback)
        lyr = QgsVectorLayer(output['Tableur'], "ogr")
        cols = [f.name() for f in lyr.fields()]
        datagen = ([f[col] for col in cols] for f in lyr.getFeatures())
        df = pd.DataFrame.from_records(data=datagen, columns=cols)
        pointidcontrat, vehiculecount, ordre = Planification(dataframe=df, pathout=pathout).fillList()
        dyn = Planification(dataframe=df, pathout=pathout).to_xlsx()
        solutionsliste = mvrp(vehiculecount, dyn, ordre, pointidcontrat)
        Solutions(mvrp_solutions=solutionsliste, pathout=pathout, pointscircuit=output['Points'], gestionnaire=self.gestionnaire, date=self.date).solutionglobalSHP()



import sys
import os.path
sys.path.append(os.getcwd())
sys.path.append('C:/OSGeo4W/apps/qgis-ltr/python')
sys.path.append('C:/OSGeo4W/apps/qgis-ltr/python/plugins')
sys.path.append('C:/Users/tclapasson/AppData/Roaming/QGIS/QGIS3/profiles/default/python/plugins/')
import processing
from processing.core.Processing import Processing
from ORStools.proc.provider import *
qgs = QgsApplication([], True)
qgs.initQgis()
Processing.initialize()
provider = ORStoolsProvider()
QgsApplication.processingRegistry().addProvider(provider)


gestionnaire = ['Robert Duplessis','Anick Laplante']
Y = "2023"
M = "02"
D = "20"
cible = 3

jours = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31']
#jours = ['20','21','22','23','24','25','26','27','28','29','30','31']
#jours = ['01','02','03','04','05','06','07','08','09','10','11']
#jours = ['01','02','03','04','05','06']
#jours = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28']
jours = ['26','27','28','29','30','31']

for i in jours:
    try:
        if datetime.date(int(Y), int(M), int(i)).weekday() == 5 or datetime.date(int(Y), int(M), int(i)).weekday() == 6:
            print("Pas de job les Week-End")
        else:
            Main(Y, M, i, gestionnaire, cible).exe()
            date = datetime.date(int(Y), int(M), int(i))
            ExportSQLServer().delete(date=date, gestionnaire=gestionnaire[0], tablename="GEOM_ITINERAIRES")
            ExportSQLServer().delete(date=date, gestionnaire=gestionnaire[0], tablename="GEOM_POINTS")
            print(datetime.date(int(Y), int(M), int(i)))
    except ValueError as ve:
        print("Jour non valide")


GroupJson().fusion()
GroupJson().ExportToSqlServer()

QgsApplication.exitQgis()