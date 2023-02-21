import os.path
import sys
sys.path.append(os.getcwd())
sys.path.append('C:/OSGeo4W/apps/qgis-ltr/python')
sys.path.append('C:/OSGeo4W/apps/qgis-ltr/python/plugins')
sys.path.append('C:/Users/tclapasson/AppData/Roaming/QGIS/QGIS3/profiles/default/python/plugins/')
from qgis.core import QgsProcessingFeedback, QgsApplication, QgsProcessingContext, QgsProcessingAlgorithm, \
    QgsVectorLayer, QgsProject, QgsProcessingParameterVectorLayer, QgsProcessing, QgsProcessingParameterField, \
    QgsProcessingParameterFeatureSource, QgsProcessingParameterEnum, QgsProcessingParameterFeatureSink, \
    QgsProcessingMultiStepFeedback, QgsExpression, QgsCoordinateReferenceSystem, QgsField, QgsFeature, \
    QgsVectorFileWriter, QgsCoordinateTransformContext, edit, QgsProcessingFeatureSourceDefinition, QgsExpressionContext, QgsExpressionContextUtils, QgsWkbTypes
from PyQt5.QtCore import QVariant
import processing
from processing.core.Processing import Processing
import csv
import pandas as pd
import glob
import os
import geojson
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
        solution_csv = Solutions(self.mvrp_solutions, self.pathout, self.pointscircuit, self.gestionnaire,self.date).solution_to_csv()
        lyr = QgsVectorLayer(self.pointscircuit, "ogr")
        pr = lyr.dataProvider()
        pr.addAttributes([QgsField('Contact', QVariant.String)])
        pr.addAttributes([QgsField('Date', QVariant.String)])
        pr.addAttributes([QgsField('Ordre', QVariant.Int)])
        pr.addAttributes([QgsField('Itineraire', QVariant.String)])
        with edit(lyr):
            for feature in lyr.getFeatures():
                feature['Contact'] = ';'.join(self.gestionnaire)
                feature['Date'] = str(self.date)
                lyr.updateFeature(feature)
        index = 0

        for Solution in solution_csv:
            index = index + 1
            solpaths = []
            with open(Solution, newline='') as f:
                reader = csv.reader(f)
                solutionliste = list(reader)
                solutionliste.remove(['0'])
            solutionlistetuple = []
            for i in solutionliste:
                solutionlistetuple.append(int(i[0]))
            solutionlistetuple = tuple(solutionlistetuple)
            if len(solutionlistetuple) > 2:
                exp = """ \"{0}" IN {1}""".format('ID', solutionlistetuple)
                processing.run("qgis:selectbyexpression", {'INPUT': lyr, 'EXPRESSION': exp, 'METHOD': 0})
                QgsVectorFileWriter.writeAsVectorFormat(lyr, self.pathout + "/Vehicule" + str(index-1) + '.geojson',"utf-8", lyr.crs(), "geoJson",onlySelected=True)
            vlayer = QgsVectorLayer(self.pathout + "/Vehicule" + str(index-1) + '.geojson', "ogr")
            hi = []
            for feat in vlayer.getFeatures():
                att = feat['Temps']
                if isinstance(att, float):
                    hi.append(att)
            somme_hi = sum(hi)
            if len(solutionliste) > 2:

                for indx, idpoint in enumerate(solutionliste):
                    try:
                        if indx+1 < len(solutionliste)-1:

                            lyr.selectByExpression(""" \"{0}" = {1}""".format('ID', solutionliste[indx+1][0]))
                            with edit(lyr):
                                for feature in lyr.selectedFeatures():
                                    feature['Ordre'] = indx + 1
                                    feature['Itineraire'] = "Itineraire"+ str(index)
                                    lyr.updateFeature(feature)


                        tupl = tuple((int(solutionliste[indx][0]), int(solutionliste[indx+1][0])))
                        exp = """ \"{0}" IN {1}""".format('ID', tupl)
                        extract = processing.run("native:extractbyexpression", {'INPUT': lyr,'EXPRESSION': exp, 'OUTPUT': 'TEMPORARY_OUTPUT'})['OUTPUT']

                        algs = {'INPUT_PROVIDER': 0,
                                 'INPUT_PROFILE': 0,
                                 'INPUT_POINT_LAYER': extract,
                                 'INPUT_LAYER_FIELD': 'ID',
                                 'INPUT_SORTBY': '',
                                 'INPUT_PREFERENCE': 0,
                                 'INPUT_OPTIMIZE': None,
                                 'INPUT_AVOID_FEATURES': [],
                                 'INPUT_AVOID_BORDERS': 0,
                                 'INPUT_AVOID_COUNTRIES': '214',
                                 'INPUT_AVOID_POLYGONS': None,
                                 'OUTPUT': 'TEMPORARY_OUTPUT'}


                        def direction(arg):
                            dir = processing.run("ORS Tools:directions_from_points_1_layer", arg)['OUTPUT']
                            file = self.pathout + "/Vehicule" + str(index - 1) + "part" + str(indx) + 'layer.geojson'
                            QgsVectorFileWriter.writeAsVectorFormat(dir, file, "utf-8", dir.crs(),"geoJson")
                            with open(file) as f:
                                gj = geojson.load(f)
                            if not gj['features']:
                                direction(arg)
                            return file,direction

                        file ,direction = direction(algs)
                        solpaths.append(file)

                    except IndexError:
                        args = {'LAYERS': solpaths,
                                'CRS': QgsCoordinateReferenceSystem('EPSG:4326'),
                                'OUTPUT': 'TEMPORARY_OUTPUT'}
                        itineraire = processing.run("native:mergevectorlayers", args)['OUTPUT']
                        QgsVectorFileWriter.writeAsVectorFormat(itineraire, self.pathout + "/Vehicule" + str(index-1) + 'layer.geojson', "utf-8", itineraire.crs(), "geoJson")
                        layer = QgsVectorLayer(self.pathout + "/Vehicule" + str(index-1) + 'layer.geojson', "ogr")
                        args = {'INPUT': layer,
                                'GROUP_BY': 'NULL',
                                'AGGREGATES': [{'aggregate': 'sum', 'delimiter': ',', 'input': '"DIST_KM"', 'length': 0,'name': 'DIST_KM', 'precision': 0, 'type': 6},
                                {'aggregate': 'sum', 'delimiter': ',', 'input': '"DURATION_H"', 'length': 0,'name': 'DURATION_H', 'precision': 0, 'type': 6}], 'OUTPUT': 'TEMPORARY_OUTPUT'}


                        vlayer = processing.run("native:aggregate", args)['OUTPUT']
                        pr = vlayer.dataProvider()
                        pr.addAttributes([QgsField('TempsTrajet', QVariant.Double)])
                        pr.addAttributes([QgsField('TempsIntervention', QVariant.Double)])
                        pr.addAttributes([QgsField('TempsTotal', QVariant.Double)])
                        #pr.addAttributes([QgsField('label_x', QVariant.Double)])
                        #pr.addAttributes([QgsField('label_y', QVariant.Double)])
                        pr.addAttributes([QgsField('Contact', QVariant.String)])
                        pr.addAttributes([QgsField('Date', QVariant.String)])
                        pr.addAttributes([QgsField('Itineraire', QVariant.String)])
                        with edit(vlayer):
                            for feature in vlayer.getFeatures():
                                feature['TempsTrajet'] = 60 * feature['DURATION_H']
                                feature['TempsIntervention'] = somme_hi
                                feature['TempsTotal'] = feature['TempsIntervention'] + feature['TempsTrajet']
                                feature['Contact'] = ';'.join(self.gestionnaire)
                                feature['Date'] = str(self.date)
                                feature['Itineraire'] = "Itineraire" + str(index)
                                vlayer.updateFeature(feature)
                        QgsVectorFileWriter.writeAsVectorFormat(vlayer,self.pathout + "/Vehicule" + str(index-1) + 'layerAgreg.geoJson', "utf-8",vlayer.crs(), "geoJson")
            else:
                print("L'itineraire " + str(index) + " n'a pas ete genere")

        return glob.glob(os.path.join(self.pathout, '*Agreg.geoJson'))

    def solutionglobalSHP(self):
        solution_shp = Solutions(self.mvrp_solutions, self.pathout, self.pointscircuit, self.gestionnaire,
                                 self.date).solution_to_shp()
        if not solution_shp:
            return (print("Erreur de regroupement des partitions d'itineraires"))
        else :
            args = {
                'LAYERS': solution_shp,
                'CRS': QgsCoordinateReferenceSystem('EPSG:4326'),
                'OUTPUT': 'TEMPORARY_OUTPUT'}
            fusion = processing.run("native:mergevectorlayers", args)['OUTPUT']


            pr = fusion.dataProvider()
            pr.addAttributes([QgsField('Geom', QVariant.String)])
            context = QgsExpressionContext()
            context.appendScopes(QgsExpressionContextUtils.globalProjectLayerScopes(fusion))
            #geom_to_wkt(geometry($currentfeature))
            with edit(fusion):
                for feature in fusion.getFeatures():
                    context.setFeature(feature)
                    feature['Geom'] = QgsExpression('geom_to_wkt(geometry($currentfeature))').evaluate(context)
                    fusion.updateFeature(feature)
            writer = QgsVectorFileWriter.writeAsVectorFormat(fusion, self.pathout + "/" + 'Itineraires.geojson', "utf-8",
                                                             fusion.crs(), "geoJson")  # ESRI Shapefile
