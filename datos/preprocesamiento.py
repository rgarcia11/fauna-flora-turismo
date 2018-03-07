import pandas as pd
import pandas_profiling

fuente1 = 'Fuente_1_procesar.csv'
fuente2 = 'Fuente_2_procesar.csv'
output1 = 'Fuente_1_procesada.csv'
output2 = 'Fuente_2_procesada_segmentos.csv'
output3 = 'Fuente_2_procesada_totales.csv'

f1 = pd.read_csv(fuente1, sep='\t|;')
print('cargado archivo fuente 1. Filas: %d ' %len(f1))

f2 = pd.read_csv(fuente2)
print('cargado archivo fuente 2. Filas: %d ' %len(f2))

#Eliminar las columnas segun el analisis de calidad de datos
#f1

print(f1.columns)
del f1['gbifid']
del f1['occurrenceid']
del f1['basisofrecord']
del f1['publishingorgkey']
del f1['coordinateuncertaintyinmeters']
del f1['coordinateprecision']
del f1['infraspecificepithet']
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
del f1['identifiedby']
del f1['recordedby']
del f1['issue']
del f1['lastinterpreted']
del f1['decimallatitude']
del f1['decimallongitude']
del f1['specieskey']
del f1['institutioncode']
del f1['collectioncode']
del f1['catalognumber']
del f1['eventdate']
del f1['species']
del f1['taxonrank']
del f1['genus']
print('borradas columnas de la fuente 1')

#borrar filas que no sean de Colombia
print('longitud archivo 1: %d'%len(f1))
indices_borrar = f1['countrycode'] != 'CO'
f1 = f1.drop(f1.index[indices_borrar])
del f1['countrycode']
indices_borrar = f1['year'] < 1995
f1 = f1.drop(f1.index[indices_borrar])
f1['year'].replace('', pd.np.nan, inplace=True)
f1.dropna(subset=['year'], inplace=True)
f1['class'].replace('', pd.np.nan, inplace=True)
f1.dropna(subset=['class'], inplace=True)
f1['order'].replace('', pd.np.nan, inplace=True)
f1.dropna(subset=['order'], inplace=True)
f1['family'].replace('', pd.np.nan, inplace=True)
f1.dropna(subset=['family'], inplace=True)
print('longitud archivo 1: %d'%len(f1))


#Quitar duplicados
f2.drop_duplicates(subset=None, inplace=True)
f1.drop_duplicates(subset=None, inplace=True)
#Arreglar tildes y simbolos especiales
f1['kingdom'] = f1['kingdom'].str.upper()
f1['phylum'] = f1['phylum'].str.upper()
f1['class'] = f1['class'].str.upper()
f1['order'] = f1['order'].str.upper()
f1['family'] = f1['family'].str.upper()
f1['scientificname'] = f1['scientificname'].str.upper()
f1['locality'] = f1['locality'].str.upper()
f1['scientificname'] = f1['scientificname'].str.upper()

f2['direccion_territorial'] = f2['direccion_territorial'].str.upper()
f2['area_protegida'] = f2['area_protegida'].str.upper()
f2['mes'] = f2['mes'].str.upper()

con_especiales = ['Á', 'É', 'Í', 'Ó', 'Ú', 'Ü', 'Ñ', 'A±',
 'Ã', 'À', 'Â', 'Ä', 'Å', 'Ç', 'È', 'Ê', 'Ë', 'Ì', 'A¡',
  'Î', 'Ï', 'Ò', 'Ô', 'Õ', 'Ö', 'Š', 'Ù', 'Û', 'Ü', 'Ý', 'Ÿ', 'Ž']
sin_especiales = ['A', 'E', 'I', 'O', 'U', 'U', 'NH', 'NH',
 'A', 'A', 'A', 'A', 'A', 'C', 'E', 'E', 'E', 'I', 'A',
  'I', 'I', 'O', 'O', 'O', 'O', 'S', 'U', 'U', 'U', 'Y', 'Y', 'Z']
f1 = f1.replace(con_especiales, sin_especiales, regex=True)
f2 = f2.replace(con_especiales, sin_especiales, regex=True)
#limpiar los nombres de direccion_territorial y area_protegida
columna_direccion_territorial = f2.columns.get_loc('direccion_territorial')
columna_area_protegida = f2.columns.get_loc('area_protegida')
for filaDT in range(0,len(f2['direccion_territorial'])):
    datico = f2.iloc[filaDT, columna_area_protegida]
    datico = datico[4:]
    if datico == 'CUEVA DE LOS GUACHAROS':
        datico = 'GUACHAROS'
    elif datico == 'ISLA DE LA COROTA':
        datico = 'COROTA'
    elif datico == 'ISLA DE SALAMANCA' or datico == 'SLA DE SALAMANCA':
        datico = 'SALAMANCA'
    elif datico == 'EL COCUY':
        datico = 'COCUY'
    elif datico == 'SIERRA NEVADA':
        datico = 'SIERRA NEVADA DE SANTA MARTA'
    elif datico == 'GUANENTA - ALTO RIO FONCE':
        datico = 'GUANENTA FONCE'
    elif datico == 'NEVADOS':
        datico = 'LOS NEVADOS'
    elif datico == 'CORALES DEL ROSARIO':
        datico = 'CORALES ROSARIO'

    f2.iloc[filaDT, columna_area_protegida] = datico
    SinDireccionTerritorial = f2.iloc[filaDT, columna_direccion_territorial]
    SinDireccionTerritorial = SinDireccionTerritorial[len("DIRECCION TERRITORIAL"):]
    f2.iloc[filaDT, columna_direccion_territorial] = SinDireccionTerritorial

def mesANumero(mes):
    if mes == 'ENERO':
        mes = 1
    elif mes == 'FEBRERO':
        mes = 2
    elif mes == 'MARZO':
        mes = 3
    elif mes == 'ABRIL':
        mes = 4
    elif mes == 'MAYO':
        mes = 5
    elif mes == 'JUNIO':
        mes = 6
    elif mes == 'JULIO':
        mes = 7
    elif mes == 'AGOSTO':
        mes = 8
    elif mes == 'SEPTIEMBRE':
        mes = 9
    elif mes == 'OCTUBRE':
        mes = 10
    elif mes == 'NOVIEMBRE':
        mes = 11
    elif mes == 'DICIEMBRE':
        mes = 12
    return mes

columna_mes = f2.columns.get_loc('mes')
for filaDT in range(0,len(f2['mes'])):
    mes = f2.iloc[filaDT, columna_mes]
    mes = mesANumero(mes)
    f2.iloc[filaDT, columna_mes] = mes

print('arreglado formato y nombres')
#Aqui se copia el f2 en f3 para poder manejar por aparte la categorizacion
f3 = f2.copy()
#categorizar las columnas que estan segmentadas por tipo de visitante
#0 -> 0
#1-100 -> 1
#101-500 -> 2
#501-1000 -> 3
#1001-5000 -> 4
#5001+ -> 5
inicio_columnas_segmentadas = f2.columns.get_loc('adultos_nacionales_2011')
fin_columnas_segmentadas = f2.columns.get_loc('total_1995')

def categorizarSegmentados(ingresos):
    if ingresos > 5000:
        ingresos = 5
    elif ingresos > 1000:
        ingresos = 4
    elif ingresos > 500:
        ingresos = 3
    elif ingresos > 100:
        ingresos = 2
    elif ingresos > 1:
        ingresos = 1
    else:
        ingresos = 0
    return ingresos

for columnaDT in range(inicio_columnas_segmentadas, fin_columnas_segmentadas):
    for filaDT in range(0, len(f2['adultos_nacionales_2011'])):
        categorizado = f2.iloc[filaDT, columnaDT]
        categorizado = categorizarSegmentados(categorizado)
        f2.iloc[filaDT, columnaDT] = categorizado

#categorizar las columnas que totalizan anualmente
#0 -> 0
#1-50 -> 1
#51-500 -> 2
#501-2000 -> 3
#2001-15000 -> 4
#15001+ -> 5
def categorizarTotalizados(ingresos):
    if ingresos > 15000:
        ingresos = 5
    elif ingresos > 2000:
        ingresos = 4
    elif ingresos > 500:
        ingresos = 3
    elif ingresos > 50:
        ingresos = 2
    elif ingresos > 1:
        ingresos = 1
    else:
        ingresos = 0
    return ingresos

inicio_columnas_totalizadas = f2.columns.get_loc('total_1995')
fin_columnas_totalizadas = f2.columns.get_loc('total_2014')+1

for columnaDT in range(inicio_columnas_totalizadas, fin_columnas_totalizadas):
    for filaDT in range(0, len(f2['total_1995'])):
        categorizado = f2.iloc[filaDT, columnaDT]
        categorizado = categorizarTotalizados(categorizado)
        f2.iloc[filaDT, columnaDT] = categorizado

print('categorizado')
#Despues de tener el archivo fuente 2 listo, comparamos el archivo fuente 1
#Queremos saber cuales avistamientos nos sirven (es decir cuales estan en los parques del archivo 2)
parques = f2['area_protegida'].unique()
parques = parques.tolist()
parques.remove('CORALES ROSARIO')
parques.append('CORALES')
parques.append('ROSARIO')
parques.remove('OTUN QUIMBAYA')
parques.append('OTUN')
parques.append('QUIMBAYA')
parques.remove('GUANENTA FONCE')
parques.append('GUANENTA')
parques.append('FONCE')

print(parques)
def buscarPalabras(localidad):
    for parque in parques:
        if parque in localidad:
            localidad = parque
            if localidad == 'CORALES' or localidad == 'ROSARIO':
                localidad = 'CORALES ROSARIO'
            elif localidad == 'OTUN' or localidad == 'QUIMBAYA':
                localidad = 'OTUN QUIMBAYA'
            elif localidad == 'GUANENTA' or localidad == 'FONCE':
                localidad = 'GUANENTA FONCE'
            return localidad
    return ""


columna_locality = f1.columns.get_loc('locality')
indices_borrar = []
for filaDT in range(0, len(f1['locality'])):
    localidad = f1.iloc[filaDT, columna_locality]
    localidad = buscarPalabras(str(localidad))
    if not localidad:
        indices_borrar.append(filaDT)
    else:
        f1.iloc[filaDT, columna_locality] = localidad

f1 = f1.drop(f1.index[indices_borrar])
print('longitud archivo 1: %d'%len(f1))

#Escribir en archivo destino
#columnas segmentadas unicamente, con sus respectivos totales
#   (es decir, todo lo que este con informacion mensual)
#columnas totalizadas anualmente
#   (es decir, todo lo que no tiene datos de todos los meses sino unicamente de enero)
f1.to_csv(output1)
headers = []
for columnaDT in range(0, f2.columns.get_loc('total_1995')):
    headers.append(f2.columns[columnaDT])

for columnaDT in range(f2.columns.get_loc('total_2009'), f2.columns.get_loc('total_2014')+1):
    headers.append(f2.columns[columnaDT])

print('arreglados headers archivo 1 fuente 2')
f2.to_csv(output2, columns = headers)


del f3['adultos_nacionales_2011']
del f3['adultos_nacionales_2012']
del f3['adultos_nacionales_2013']
del f3['adultos_nacionales_2014']
del f3['ninos_y_estudiantes_2011']
del f3['ninos_y_estudiantes_2012']
del f3['ninos_y_estudiantes_2013']
del f3['ninos_y_estudiantes_2014']
del f3['extranjeros_2011']
del f3['extranjeros_2012']
del f3['extranjeros_2013']
del f3['extranjeros_2014']
del f3['extranjeros_residentes_2011']
del f3['extranjeros_residentes_2012']
del f3['extranjeros_residentes_2013']
del f3['extranjeros_residentes_2014']
del f3['ninos_residentes_2011']
del f3['ninos_residentes_2012']
del f3['ninos_residentes_2013']
del f3['ninos_residentes_2014']
del f3['adultos_residentes_2011']
del f3['adultos_residentes_2012']
del f3['adultos_residentes_2013']
del f3['adultos_residentes_2014']
del f3['ninos_excentos_no_residentes_2011']
del f3['ninos_excentos_no_residentes_2012']
del f3['ninos_excentos_no_residentes_2013']
del f3['ninos_excentos_no_residentes_2014']
del f3['adultos_excentos_no_residentes_2011']
del f3['adultos_excentos_no_residentes_2012']
del f3['adultos_excentos_no_residentes_2013']
del f3['adultos_excentos_no_residentes_2014']

print('borradas columnas archivo 2 fuente 2')
print('filas del tercer archivo antes de sumar por mes: %d' %len(f3))
todas_las_columnas = list(f3.columns)
todas_las_columnas.remove('direccion_territorial')
todas_las_columnas.remove('area_protegida')
todas_las_columnas.remove('mes')
f3 = f3.groupby(['direccion_territorial', 'area_protegida'])[todas_las_columnas].sum()
print('filas del tercer archivo despues de sumar por mes: %d' %len(f3))
f3.to_csv(output3)
