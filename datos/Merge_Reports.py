

fuente_1 = pd.read_csv("./Fuente_1_procesada.csv", error_bad_lines = False, sep = ";", encoding = "UTF-8")
fuente_2_segmentos = pd.read_csv("./Fuente_2_procesada_segmentos.csv", error_bad_lines = False, sep = ",", encoding = "UTF-8")
fuente_2_totales = pd.read_csv("./Fuente_2_procesada_totales.csv", error_bad_lines = False, sep = ",", encoding = "UTF-8")

pf1 = pandas_profiling.ProfileReport(fuente_1)
pf2 = pandas_progiling.ProfileReport(fuente_2_segmentos)
pf3 = pandas_progiling.ProfileReport(fuente_2_totales)

pf1.to_file(outputfile="./fuente_1_procesada.html")
pf2.to_file(outputfile="./fuente_2_procesada_segmentos.html")
pf3.to_file(outputfile="./fuente_2_procesada_totales.html")