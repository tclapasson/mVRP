from qgis.core import QgsProcessingFeedback, QgsApplication, QgsProcessingContext, QgsProcessingAlgorithm, \
    QgsVectorLayer, QgsProject, QgsProcessingParameterVectorLayer, QgsProcessing, QgsProcessingParameterField, \
    QgsProcessingParameterFeatureSource, QgsProcessingParameterEnum, QgsProcessingParameterFeatureSink, \
    QgsProcessingMultiStepFeedback, QgsExpression, QgsCoordinateReferenceSystem, QgsField, QgsFeature, QgsVectorFileWriter, QgsCoordinateTransformContext, edit
from PyQt5.QtCore import QVariant
import processing
from processing.core.Processing import Processing

class Modle(QgsProcessingAlgorithm):

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterVectorLayer('CSV', 'CSV', types=[QgsProcessing.TypeVector], defaultValue=None))
        self.addParameter(QgsProcessingParameterField('X', 'X', type=QgsProcessingParameterField.Any, parentLayerParameterName='CSV', allowMultiple=False, defaultValue=None))
        self.addParameter(QgsProcessingParameterField('Y', 'Y', type=QgsProcessingParameterField.Any, parentLayerParameterName='CSV', allowMultiple=False, defaultValue=None))
        self.addParameter(QgsProcessingParameterFeatureSink('Points', 'Points', type=QgsProcessing.TypeVectorAnyGeometry, createByDefault=True, supportsAppend=True, defaultValue=None))
        self.addParameter(QgsProcessingParameterFeatureSink('Tableur', 'Tableur', type=QgsProcessing.TypeVectorAnyGeometry, createByDefault=True, supportsAppend=True, defaultValue=None))

    def processAlgorithm(self, parameters, context, model_feedback):
        # Use a multi-step feedback, so that individual child algorithm progress reports are adjusted for the
        # overall progress through the model
        feedback = QgsProcessingMultiStepFeedback(10, model_feedback)
        results = {}
        outputs = {}

        # Couche à partir de coordonnées
        alg_params = {
            'INPUT': parameters['CSV'],
            'MFIELD': '',
            'TARGET_CRS': QgsCoordinateReferenceSystem('EPSG:4326'),
            'XFIELD': parameters['X'],
            'YFIELD': parameters['Y'],
            'ZFIELD': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['CouchePartirDeCoordonnes'] = processing.run('native:createpointslayerfromtable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(1)
        if feedback.isCanceled():
            return {}

        # Refactoriser Int Jointure
        alg_params = {
            'FIELDS_MAPPING': [{'expression': '"field_1"','length': 0,'name': 'ID','precision': 0,'type': 2},{'expression': '"Temps"','length': 0,'name': 'Temps','precision': 0,'type': 6},{'expression': '"ID_EQUIPMENT"','length': 0,'name': 'ID_EQUIPMENT','precision': 0,'type': 10},{'expression': '"NO_EQUIPMENT"','length': 0,'name': 'NO_EQUIPMENT','precision': 0,'type': 10},{'expression': '"ID_PLANT"','length': 0,'name': 'ID_PLANT','precision': 0,'type': 10},{'expression': '"GPS_LONGITUDE"','length': 0,'name': 'GPS_LONGITUDE','precision': 0,'type': 6},{'expression': '"GPS_LATITUDE"','length': 0,'name': 'GPS_LATITUDE','precision': 0,'type': 6},{'expression': '"ID_SIMO_PLANT_TOUR_CONFIGURATIONS"','length': 0,'name': 'ID_SIMO_PLANT_TOUR_CONFIGURATIONS','precision': 0,'type': 10},{'expression': '"FREQUENCE"','length': 0,'name': 'FREQUENCE','precision': 0,'type': 10},{'expression': '"OCCURENCE"','length': 0,'name': 'OCCURENCE','precision': 0,'type': 10},{'expression': '"ID_PLANT_1"','length': 0,'name': 'ID_PLANT_1','precision': 0,'type': 6},{'expression': '"CITY"','length': 0,'name': 'CITY','precision': 0,'type': 10},{'expression': '"DESCRIPTION"','length': 0,'name': 'DESCRIPTION','precision': 0,'type': 10},{'expression': '"CONTACT_NAME"','length': 0,'name': 'CONTACT_NAME','precision': 0,'type': 10},{'expression': '"NbVehicle"','length': 0,'name': 'NbVehicle','precision': 0,'type': 6},{'expression': '"Id"','length': 0,'name': 'Id','precision': 0,'type': 6},{'expression': '"F_ACTIVE"','length': 0,'name': 'F_ACTIVE','precision': 0,'type': 6},{'expression': '"TachesPLANT_TOUR"','length': 10000,'name': 'TachesPLANT_TOUR','precision': 0,'type': 10},{'expression': '"TachesWORK_ORDER"','length': 10000,'name': 'TachesWORK_ORDER','precision': 0,'type': 10}],
            'INPUT': outputs['CouchePartirDeCoordonnes']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['RefactoriserIntJointure'] = processing.run('native:refactorfields', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(2)
        if feedback.isCanceled():
            return {}

        # ORS Matrix from layers
        alg_params = {
            'INPUT_END_FIELD': 'ID',
            'INPUT_END_LAYER': outputs['RefactoriserIntJointure']['OUTPUT'],
            'INPUT_PROFILE': 0,  # driving-car
            'INPUT_PROVIDER': 0,  # openrouteservice
            'INPUT_START_FIELD': 'ID',
            'INPUT_START_LAYER': outputs['RefactoriserIntJointure']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }

        def matrix(arg):
            try:
                outputs['OrsMatrixFromLayers'] = processing.run('ORS Tools:matrix_from_layers', arg,context=context, feedback=feedback,is_child_algorithm=True)
            except:
                matrix(arg)

        matrix(alg_params)

        feedback.setCurrentStep(3)
        if feedback.isCanceled():
            return {}

        # int DURATION et DIST_KM
        alg_params = {
            'FIELDS_MAPPING': [{'expression': '"FROM_ID"','length': 0,'name': 'FROM_ID','precision': 0,'type': 2},{'expression': '"TO_ID"','length': 0,'name': 'TO_ID','precision': 0,'type': 2},{'expression': 'ceil("DURATION_H"*60)','length': 0,'name': 'DURATION_H','precision': 0,'type': 6},{'expression': 'ceil("DIST_KM")','length': 0,'name': 'DIST_KM','precision': 0,'type': 6}],
            'INPUT': outputs['OrsMatrixFromLayers']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['IntDurationEtDist_km'] = processing.run('native:refactorfields', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(4)
        if feedback.isCanceled():
            return {}

        # Jointure source matrice
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'TO_ID',
            'FIELDS_TO_COPY': [''],
            'FIELD_2': 'ID',
            'INPUT': outputs['IntDurationEtDist_km']['OUTPUT'],
            'INPUT_2': outputs['RefactoriserIntJointure']['OUTPUT'],
            'METHOD': 0,  # Créer une entité distincte pour chaque entité correspondante (un à plusieurs)
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['JointureSourceMatrice'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(5)
        if feedback.isCanceled():
            return {}

        # Ajout DURATION_M_TOT
        alg_params = {
            'FIELDS_MAPPING': [{'expression': '"FROM_ID"','length': 0,'name': 'FROM_ID','precision': 0,'type': 2},{'expression': '"TO_ID"','length': 0,'name': 'TO_ID','precision': 0,'type': 2},{'expression': '"DURATION_H"','length': 0,'name': 'DURATION_H','precision': 0,'type': 6},{'expression': '"DIST_KM"','length': 0,'name': 'DIST_KM','precision': 0,'type': 6},{'expression': '"ID"','length': 0,'name': 'ID','precision': 0,'type': 2},{'expression': '"Temps"','length': 0,'name': 'Temps','precision': 0,'type': 6},{'expression': '"ID_EQUIPMENT"','length': 0,'name': 'ID_EQUIPMENT','precision': 0,'type': 10},{'expression': '"NO_EQUIPMENT"','length': 0,'name': 'NO_EQUIPMENT','precision': 0,'type': 10},{'expression': '"ID_PLANT"','length': 0,'name': 'ID_PLANT','precision': 0,'type': 10},{'expression': '"GPS_LONGITUDE"','length': 0,'name': 'GPS_LONGITUDE','precision': 0,'type': 6},{'expression': '"GPS_LATITUDE"','length': 0,'name': 'GPS_LATITUDE','precision': 0,'type': 6},{'expression': '"ID_SIMO_PLANT_TOUR_CONFIGURATIONS"','length': 0,'name': 'ID_SIMO_PLANT_TOUR_CONFIGURATIONS','precision': 0,'type': 10},{'expression': '"FREQUENCE"','length': 0,'name': 'FREQUENCE','precision': 0,'type': 10},{'expression': '"OCCURENCE"','length': 0,'name': 'OCCURENCE','precision': 0,'type': 10},{'expression': '"CITY"','length': 0,'name': 'CITY','precision': 0,'type': 10},{'expression': '"DESCRIPTION"','length': 0,'name': 'DESCRIPTION','precision': 0,'type': 10},{'expression': '"CONTACT_NAME"','length': 0,'name': 'CONTACT_NAME','precision': 0,'type': 10},{'expression': '"NbVehicle"','length': 0,'name': 'NbVehicle','precision': 0,'type': 6},{'expression': '"Id"','length': 0,'name': 'Id','precision': 0,'type': 6},{'expression': '"F_ACTIVE"','length': 0,'name': 'F_ACTIVE','precision': 0,'type': 6},{'expression': 'if("TO_ID" <> "FROM_ID",if("ID_EQUIPMENT" IS NULL ,"DURATION_H", "DURATION_H" + "Temps") , 0)','length': 0,'name': 'DURATION_M_TOT','precision': 0,'type': 2},{'expression': '"TachesPLANT_TOUR"','length': 10000,'name': 'TachesPLANT_TOUR','precision': 0,'type': 10},{'expression': '"TachesWORK_ORDER"','length': 10000,'name': 'TachesWORK_ORDER','precision': 0,'type': 10}],
            'INPUT': outputs['JointureSourceMatrice']['OUTPUT'],
            'OUTPUT': parameters['Tableur']
        }
        outputs['AjoutDuration_m_tot'] = processing.run('native:refactorfields', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['Tableur'] = outputs['AjoutDuration_m_tot']['OUTPUT']

        feedback.setCurrentStep(6)
        if feedback.isCanceled():
            return {}

        # points
        alg_params = {
            'INPUT': outputs['AjoutDuration_m_tot']['OUTPUT'],
            'MFIELD': '',
            'TARGET_CRS': QgsCoordinateReferenceSystem('EPSG:4326'),
            'XFIELD': 'GPS_LONGITUDE',
            'YFIELD': 'GPS_LATITUDE',
            'ZFIELD': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['Points'] = processing.run('native:createpointslayerfromtable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(7)
        if feedback.isCanceled():
            return {}

        # Supprimer les doublons par attribut
        alg_params = {
            'FIELDS': ['ID'],
            'INPUT': outputs['Points']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['SupprimerLesDoublonsParAttribut'] = processing.run('native:removeduplicatesbyattribute', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(8)
        if feedback.isCanceled():
            return {}

        # x_label
        alg_params = {
            'FIELD_LENGTH': 10,
            'FIELD_NAME': 'x_label',
            'FIELD_PRECISION': 4,
            'FIELD_TYPE': 1,  # Flottant
            'INPUT': outputs['SupprimerLesDoublonsParAttribut']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['X_label'] = processing.run('native:addfieldtoattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(9)
        if feedback.isCanceled():
            return {}

        # y_label
        alg_params = {
            'FIELD_LENGTH': 10,
            'FIELD_NAME': 'y_label',
            'FIELD_PRECISION': 4,
            'FIELD_TYPE': 1,  # Flottant
            'INPUT': outputs['X_label']['OUTPUT'],
            'OUTPUT': parameters['Points']
        }
        outputs['Y_label'] = processing.run('native:addfieldtoattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['Points'] = outputs['Y_label']['OUTPUT']
        return results

    def name(self):
        return 'Modèle'

    def displayName(self):
        return 'Modèle'

    def group(self):
        return ''

    def groupId(self):
        return ''

    def createInstance(self):
        return Modle()

