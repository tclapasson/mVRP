from qgis.core import QgsProcessingFeedback, QgsApplication, QgsProcessingContext, QgsProcessingAlgorithm, \
    QgsVectorLayer, QgsProject, QgsProcessingParameterVectorLayer, QgsProcessing, QgsProcessingParameterField, \
    QgsProcessingParameterFeatureSource, QgsProcessingParameterEnum, QgsProcessingParameterFeatureSink, \
    QgsProcessingMultiStepFeedback, QgsExpression, QgsCoordinateReferenceSystem, QgsField, QgsFeature, \
    QgsVectorFileWriter, QgsCoordinateTransformContext, edit
from PyQt5.QtCore import QVariant
import processing
from processing.core.Processing import Processing
import csv
import pandas as pd
import glob
import os
import datetime


class Solutions:
    def __init__(self, mvrp_solutions, pathout, pointscircuit, gestionnaire, date):
        self.mvrp_solutions = mvrp_solutions
        self.pathout = pathout
        self.pointscircuit = pointscircuit
        self.gestionnaire = gestionnaire
        self.date = date

    def solution_to_csv(self):
        for index, x in self.mvrp_solutions.items():
            df = pd.DataFrame(x)
            df.to_csv(self.pathout + "/Vehicule" + str(index) + ".csv", index=False)
        return glob.glob(os.path.join(self.pathout, 'Vehicule*.csv'))

    def solution_to_shp(self):
        index = 0
        solution_csv = Solutions(self.mvrp_solutions, self.pathout, self.pointscircuit, self.gestionnaire,
                                 self.date).solution_to_csv()
        for Solution in solution_csv:
            with open(Solution, newline='') as f:
                reader = csv.reader(f)
                solutionliste = list(reader)
                solutionliste.remove(['0'])
            lyr = QgsVectorLayer(self.pointscircuit, "ogr")

            pr = lyr.dataProvider()
            pr.addAttributes([QgsField('Contact', QVariant.String)])
            pr.addAttributes([QgsField('Date', QVariant.String)])
            with edit(lyr):
                for feature in lyr.getFeatures():
                    feature['Contact'] = ';'.join(self.gestionnaire)
                    feature['Date'] = str(self.date)
                    lyr.updateFeature(feature)


            solutionlistetuple = []
            for i in solutionliste:
                solutionlistetuple.append(int(i[0]))
            solutionlistetuple = tuple(solutionlistetuple)

            if len(solutionlistetuple) > 2:
                exp = """ \"{0}" IN {1}""".format('ID', solutionlistetuple)
                processing.run("qgis:selectbyexpression", {'INPUT': lyr, 'EXPRESSION': exp, 'METHOD': 1})
                writer = QgsVectorFileWriter.writeAsVectorFormat(lyr, self.pathout + "/Vehicule" + str(index) + '.shp',
                                                                 "utf-8", lyr.crs(), "ESRI Shapefile",
                                                                 onlySelected=True)

                hi = []
                for feat in lyr.selectedFeatures():
                    att = feat['Temps']
                    if isinstance(att, int):
                        hi.append(att)
                somme_hi = sum(hi)

                lyr.removeSelection()
                vlayer = QgsVectorLayer(self.pathout + "/Vehicule" + str(index) + '.shp', "ogr")

                algs = {'INPUT_PROVIDER': 0,
                        'INPUT_PROFILE': 0,
                        'INPUT_POINT_LAYER': vlayer,
                        'INPUT_LAYER_FIELD': 'ID',
                        'INPUT_SORTBY': 'FROM_ID',
                        'INPUT_PREFERENCE': 0,
                        'INPUT_OPTIMIZE': 0,
                        'INPUT_AVOID_FEATURES': [],
                        'INPUT_AVOID_BORDERS': 0,
                        'INPUT_AVOID_COUNTRIES': '214',
                        'INPUT_AVOID_POLYGONS': None,
                        'OUTPUT': 'TEMPORARY_OUTPUT'}

                direction = processing.run("ORS Tools:directions_from_points_1_layer", algs)['OUTPUT']
                #for row in direction.getFeatures():
                 #   print(row['ID'])
                pr = direction.dataProvider()
                pr.addAttributes([QgsField('TempsTrajet', QVariant.Int)])
                pr.addAttributes([QgsField('TempsIntervention', QVariant.Int)])
                pr.addAttributes([QgsField('TempsTotal', QVariant.Int)])
                #pr.addAttributes([QgsField('label_x', QVariant.Double)])
                #pr.addAttributes([QgsField('label_y', QVariant.Double)])
                pr.addAttributes([QgsField('Contact', QVariant.String)])
                pr.addAttributes([QgsField('Date', QVariant.String)])
                with edit(direction):
                    for feature in direction.getFeatures():
                        feature['TempsTrajet'] = 60 * feature['DURATION_H']
                        feature['TempsIntervention'] = somme_hi
                        feature['TempsTotal'] = feature['TempsIntervention'] + feature['TempsTrajet']
                        feature['Contact'] = ';'.join(self.gestionnaire)
                        feature['Date'] = str(self.date)
                        direction.updateFeature(feature)
                writer = QgsVectorFileWriter.writeAsVectorFormat(direction,
                                                                 self.pathout + "/Vehicule" + str(
                                                                     index) + 'layer.shp', "utf-8",
                                                                 direction.crs(), "ESRI Shapefile")
                index = index + 1
            else:
                index = index + 1
        return glob.glob(os.path.join(self.pathout, '*layer.shp'))

    def solutionglobalSHP(self):
        solution_shp = Solutions(self.mvrp_solutions, self.pathout, self.pointscircuit, self.gestionnaire,
                                 self.date).solution_to_shp()
        args = {
            'LAYERS': solution_shp,
            'CRS': QgsCoordinateReferenceSystem('EPSG:4326'),
            'OUTPUT': 'TEMPORARY_OUTPUT'}
        fusion = processing.run("native:mergevectorlayers", args)['OUTPUT']
        writer = QgsVectorFileWriter.writeAsVectorFormat(fusion, self.pathout + "/" + 'Itineraires.geojson', "utf-8",
                                                         fusion.crs(), "geoJson")  # ESRI Shapefile
