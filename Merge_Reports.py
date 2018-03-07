import pandas as pd
import pandas_profiling

fuente_1 = pd.read_csv("./Fuente_1_procesada.csv", error_bad_lines = False, sep = ";", encoding = "UTF-8")
fuente_2_segmentos = pd.read_csv("./Fuente_2_procesada_segmentos.csv", error_bad_lines = False, sep = ",", encoding = "UTF-8")
fuente_2_totales = pd.read_csv("./Fuente_2_procesada_totales.csv", error_bad_lines = False, sep = ",", encoding = "UTF-8")

pf1 = pandas_profiling.ProfileReport(fuente_1)
pf2 = pandas_profiling.ProfileReport(fuente_2_segmentos)
pf3 = pandas_profiling.ProfileReport(fuente_2_totales)

pf1.to_file(outputfile="./fuente_1_procesada.html")
pf2.to_file(outputfile="./fuente_2_procesada_segmentos.html")
pf3.to_file(outputfile="./fuente_2_procesada_totales.html")

#merge

left = pd.DataFrame({'key': fuente_1['locality'], 
    'kingdom': fuente_1['kingdom'],
    'phylum': fuente_1['phylum'], 
    'class': fuente_1['class'], 
    'order': fuente_1['order'],
    'family': fuente_1['family'], 
    'scientificname': fuente_1['scientificname'], 
    'month': fuente_1['month'], 
    'year': fuente_1['year'], 
    'taxonkey': fuente_1['taxonkey']})



right = pd.DataFrame({'key': fuente_2_segmentos['area_protegida'], 
    'direccion_territorial': fuente_2_segmentos['direccion_territorial'], 
    'mes': fuente_2_segmentos['mes'], 
    'adultos_nacionales_2011': fuente_2_segmentos['adultos_nacionales_2011'], 
    'ninos_y_estudiantes_2011': fuente_2_segmentos['ninos_y_estudiantes_2011'],
    'extranjeros_2011': fuente_2_segmentos['extranjeros_2011'], 
    'extranjeros_residentes_2011': fuente_2_segmentos['extranjeros_residentes_2011'],
    'ninos_residentes_2011': fuente_2_segmentos['ninos_residentes_2011'],
    'adultos_residentes_2011': fuente_2_segmentos['adultos_residentes_2011'],
    'ninos_excentos_no_residentes_2011': fuente_2_segmentos['ninos_excentos_no_residentes_2011'],
    'adultos_excentos_no_residentes_2011': fuente_2_segmentos['adultos_excentos_no_residentes_2011'],
    'adultos_nacionales_2012': fuente_2_segmentos['adultos_nacionales_2012'],
    'ninos_y_estudiantes_2012': fuente_2_segmentos['ninos_y_estudiantes_2012'], 
    'extranjeros_2012': fuente_2_segmentos['extranjeros_2012'],
    'extranjeros_residentes_2012': fuente_2_segmentos['extranjeros_residentes_2012'], 
    'ninos_residentes_2012': fuente_2_segmentos['ninos_residentes_2012'],
    'adultos_residentes_2012': fuente_2_segmentos['adultos_residentes_2012'],
    'ninos_excentos_no_residentes_2012': fuente_2_segmentos['ninos_excentos_no_residentes_2012'],
    'adultos_excentos_no_residentes_2012': fuente_2_segmentos['adultos_excentos_no_residentes_2012'], 
    'adultos_nacionales_2013': fuente_2_segmentos['adultos_nacionales_2013'], 
    'ninos_y_estudiantes_2013': fuente_2_segmentos['ninos_y_estudiantes_2013'],
    'extranjeros_2013': fuente_2_segmentos['extranjeros_2013'], 
    'extranjeros_residentes_2013': fuente_2_segmentos['extranjeros_residentes_2013'],
    'ninos_residentes_2013': fuente_2_segmentos['ninos_residentes_2013'],
    'adultos_residentes_2013': fuente_2_segmentos['adultos_residentes_2013'], 
    'ninos_excentos_no_residentes_2013': fuente_2_segmentos['ninos_excentos_no_residentes_2013'],
    'adultos_excentos_no_residentes_2013': fuente_2_segmentos['adultos_excentos_no_residentes_2013'], 
    'adultos_nacionales_2014': fuente_2_segmentos['adultos_nacionales_2014'],
    'ninos_y_estudiantes_2014': fuente_2_segmentos['ninos_y_estudiantes_2014'], 
    'extranjeros_2014': fuente_2_segmentos['extranjeros_2014'],
    'extranjeros_residentes_2014': fuente_2_segmentos['extranjeros_residentes_2014'], 
    'ninos_residentes_2014': fuente_2_segmentos['ninos_residentes_2014'],
    'adultos_residentes_2014': fuente_2_segmentos['adultos_residentes_2014'], 
    'ninos_excentos_no_residentes_2014': fuente_2_segmentos['ninos_excentos_no_residentes_2014'],
    'adultos_excentos_no_residentes_2014': fuente_2_segmentos['adultos_excentos_no_residentes_2014'],
    'total_2009': fuente_2_segmentos['total_2009'], 
    'total_2010': fuente_2_segmentos['total_2010'],
    'total_2011': fuente_2_segmentos['total_2011'], 
    'total_2012': fuente_2_segmentos['total_2012'],
    'total_2013': fuente_2_segmentos['total_2013'], 
    'total_2014': fuente_2_segmentos['total_2014']
    })

#Segmentos vs fuente 1
res1 = pd.merge(left, right, on= 'key')

right = pd.DataFrame({'key': fuente_2_totales['area_protegida'],
    'direccion_territorial': fuente_2_totales['direccion_territorial'], 
    'total_1995': fuente_2_totales['total_1995'], 
    'total_1996': fuente_2_totales['total_1996'],
    'total_1997': fuente_2_totales['total_1997'], 
    'total_1998': fuente_2_totales['total_1998'],
    'total_1999': fuente_2_totales['total_1999'], 
    'total_2000': fuente_2_totales['total_2000'],
    'total_2001': fuente_2_totales['total_2001'],
    'total_2002': fuente_2_totales['total_2002'], 
    'total_2003': fuente_2_totales['total_2003'], 
    'total_2004': fuente_2_totales['total_2004'], 
    'total_2005': fuente_2_totales['total_2005'], 
    'total_2006': fuente_2_totales['total_2006'],
    'total_2007': fuente_2_totales['total_2007'], 
    'total_2008': fuente_2_totales['total_2008'], 
    'total_2009': fuente_2_totales['total_2009'], 
    'total_2010': fuente_2_totales['total_2010'], 
    'total_2011': fuente_2_totales['total_2011'],
    'total_2012': fuente_2_totales['total_2012'],
    'total_2013': fuente_2_totales['total_2013'], 
    'total_2014': fuente_2_totales['total_2014']
    })

#Totales vs fuente 1
res2 = pd.merge(left, right, on= 'key')

#save to new csv
res1.to_csv('./merge_fuente1_segmentos.csv', sep=',')
res2.to_csv('./merge_fuente1_totales.csv', sep=',')
