import openrouteservice as ors
import folium
import geojson
from qgis.gui import QgsLayerTreeMapCanvasBridge, QgsMapCanvas
from qgis.core import QgsProcessingFeedback, QgsApplication, QgsProcessingContext, QgsProcessingAlgorithm, \
    QgsVectorLayer, QgsProject, QgsProcessingParameterVectorLayer, QgsProcessing, QgsProcessingParameterField, \
    QgsProcessingParameterFeatureSource, QgsProcessingParameterEnum, QgsProcessingParameterFeatureSink, \
    QgsProcessingMultiStepFeedback, QgsExpression, QgsCoordinateReferenceSystem, QgsField, QgsFeature, QgsVectorFileWriter, QgsCoordinateTransformContext, edit
from PyQt5.QtCore import QVariant


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



client = ors.Client(key='5b3ce3597851110001cf62489b2190521f494f2eb6de1b37d725c551')

m = folium.Map(location=[52.521861, 13.40744], tiles='cartodbpositron', zoom_start=13)

# Some coordinates in Berlin
coordinates = [[13.42731, 52.51088], [13.384116, 52.533558]]

route = client.directions(
    coordinates=coordinates,
    profile='foot-walking',
    format='geojson',
    options={"avoid_features": ["steps"]},
    validate=False,
)
folium.PolyLine(locations=[list(reversed(coord))
                           for coord in
                           route['features'][0]['geometry']['coordinates']]).add_to(m)


feature = geojson.FeatureCollection(route['features'])
feature = geojson.LineString(route['features'][0]['geometry']['coordinates'])
feature = geojson.FeatureCollection(route['features'][0]['geometry']['coordinates'])

QgsVectorFileWriter.writeAsVectorFormat(feature, 'C:/Users/tclapasson/Downloadstets/TETS.shp', "utf-8",
                                                         "EPSG:4326", "ESRI Shapefile")

