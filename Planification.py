import pandas as pd
import numpy
import openpyxl

class Planification:
    def __init__(self, dataframe, pathout):
        self.pathout = pathout
        self.dataframe = dataframe
        self.Ordre = [[], []]
        self.ID_DEPART = set()
        self.unique = set()
        self.VehiculeCount = 0
        self.pointIdcontrat = []

    def fillList(self):
        for index, row in self.dataframe.iterrows():
            if row['DESCRIPTION'] == None:
                self.ID_DEPART.add(row['ID'])
            else:
                self.unique.add(row['DESCRIPTION'])
        for id in self.ID_DEPART:
            nombrevehic = (set(self.dataframe.loc[self.dataframe['ID'] == id, 'NbVehicle'].to_numpy()))
            vehicule = int((list(nombrevehic)[0]))
            self.VehiculeCount = self.VehiculeCount + vehicule
            for i in range(vehicule):
                self.Ordre[0].append(int(id))
                self.Ordre[1].append(int(id))
        #### [[56, 54, 55, 55], [56, 54, 55, 55]]
        for x in self.unique:
            liste = []
            for y in set(self.dataframe.loc[self.dataframe['DESCRIPTION'] == x, 'ID'].to_numpy()):
                liste.append(int(y))
            self.pointIdcontrat.append(liste)
        #   pointIdcontrat.remove([0]) ## ==[2,4,8],[3],[7,9,10,11,12],[3,5],[1],[6]]
        return self.pointIdcontrat, self.VehiculeCount, self.Ordre

    def pivot(self):
        data = self.dataframe.pivot_table("DURATION_M_TOT", index='FROM_ID', columns='TO_ID', aggfunc='sum', margins=True)
        data.drop(data.columns[len(data.columns) - 1], axis=1, inplace=True)
        data.drop(data.tail(1).index, inplace=True)
        return data, data.columns

    def to_xlsx(self):
        df, col = Planification(self.dataframe, self.pathout).pivot()
        dataframe = pd.DataFrame.from_records(data=df, columns=col)
        writer = pd.ExcelWriter(self.pathout + "/Matrice.xlsx")
        dataframe.to_excel(writer)
        writer.save()
        Numpy = dataframe.to_numpy()
        Numpylist = Numpy.tolist()
        return Numpylist
