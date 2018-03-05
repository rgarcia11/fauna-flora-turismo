import pandas as pd
import pandas_profiling

fuente1 = 'Fuente_1_procesar.csv'
fuente2 = 'Fuente_2_procesar.csv'
output1 = 'Fuente_1_procesada.csv'
output2 = 'Fuente_2_procesada.csv'

f1 = pd.read_csv(fuente1, sep='\t|;')
print(len(f1))
print(f1.head())
f2 = pd.read_csv(fuente2)
print(len(f2))
print(f2.head())

#Eliminar las columnas segun el analisis de calidad de datos
#f1

print(f1.columns)
del f1['basisofrecord']
del f1['coordinateuncertaintyinmeters']
del f1['coordinateprecision']
del f1['datasetkey']
del f1['day']
del f1['depth']
del f1['depthaccuracy']
del f1['elevation']
del f1['elevationaccuracy']
del f1['establishmentmeans']
del f1['license']
del f1['mediatype']
del f1['recordnumber']
del f1['rightsholder']
del f1['typestatus']

print(f1.head())
#f2
#Quitar duplicados
f2.drop_duplicates(subset=None, inplace=True)
#Arreglar tildes y simbolos especiales
f2['direccion_territorial'] = f2['direccion_territorial'].str.upper()
f2['area_protegida'] = f2['area_protegida'].str.upper()
f2['mes'] = f2['mes'].str.upper()
con_especiales = ['Á', 'É', 'Í', 'Ó', 'Ú', 'Ü', 'Ñ']
sin_especiales = ['A', 'E', 'I', 'O', 'U', 'U', 'NH']
f2 = f2.replace(con_especiales, sin_especiales, regex=True)
#categorizar

#Escribir en archivo destino
f2.to_csv(output2)
