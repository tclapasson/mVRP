import os
import os.path
import sys
from qgis.gui import QgsLayerTreeMapCanvasBridge, QgsMapCanvas
from qgis.core import QgsProcessingFeedback, QgsApplication, QgsProcessingContext, QgsProcessingAlgorithm, \
    QgsVectorLayer, QgsProject, QgsProcessingParameterVectorLayer, QgsProcessing, QgsProcessingParameterField, \
    QgsProcessingParameterFeatureSource, QgsProcessingParameterEnum, QgsProcessingParameterFeatureSink, \
    QgsProcessingMultiStepFeedback, QgsExpression, QgsCoordinateReferenceSystem, QgsField, QgsFeature, QgsVectorFileWriter, QgsCoordinateTransformContext, edit
from PyQt5.QtCore import QVariant
import processing
from processing.core.Processing import Processing
from ExportDatabase import ExportSQLServer

class GroupJson:
    def __init__(self):
        self.pathGJson = []
        self.pathSolOut = os.getcwd() + "\\" + 'GJSON\\ItinerairesHelios.geojson'
        self.pathPoint = []
        self.pathPointOut = os.getcwd() + "\\" + 'GJSON\\PointsHelios.geojson'
    def fusion(self):
        for root, directories, files in os.walk("."):
            for file in files:
                if file == 'Itineraires.geojson':
                    self.pathGJson.append(os.getcwd() + root.replace('.', '') + "\\" + file)
                if file == 'Points.geojson':
                    self.pathPoint.append(os.getcwd() + root.replace('.', '') + "\\" + file)
        print(self.pathGJson)
        args = {'LAYERS': self.pathGJson,
                'CRS': QgsCoordinateReferenceSystem('EPSG:4326'),
                'OUTPUT': 'TEMPORARY_OUTPUT'}
        fusion = processing.run("native:mergevectorlayers", args)

        if os.path.exists(self.pathSolOut):
            os.remove(self.pathSolOut)
        elif os.path.exists(self.pathPointOut):
            os.remove(self.pathPointOut)
        else:
            os.mkdir((os.getcwd() + "\\" + 'GJSON'))



        QgsVectorFileWriter.writeAsVectorFormat(fusion['OUTPUT'], self.pathSolOut, "utf-8",
                                                         fusion['OUTPUT'].crs(), "GeoJson")  # ESRI Shapefile

        args = {'LAYERS': self.pathPoint,
                'CRS': QgsCoordinateReferenceSystem('EPSG:4326'),
                'OUTPUT': 'TEMPORARY_OUTPUT'}
        fusion = processing.run("native:mergevectorlayers", args)

        QgsVectorFileWriter.writeAsVectorFormat(fusion['OUTPUT'], self.pathPointOut, "utf-8",
                                                         fusion['OUTPUT'].crs(), "GeoJson")  # ESRI Shapefile

    def ExportToSqlServer(self):
        ExportSQLServer().export(self.pathSolOut, "GEOM_ITINERAIRES")
        ExportSQLServer().export(self.pathPointOut, "GEOM_POINTS")


QgsApplication.exitQgis()

