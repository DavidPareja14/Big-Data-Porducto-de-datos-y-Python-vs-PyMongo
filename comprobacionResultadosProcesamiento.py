#python comprobacionResultadosProcesamiento.py pruebaFinal datosPrueba cantidadLineas !camposATrabajar!

from collections import defaultdict
import sys

baseDatos = sys.argv[1] #el nombre de la base de datos donde subi las particiones y el padre.
archivoPadre = sys.argv[2] #el archivo del cual creo las particiones (sin extension).

numIteraciones = int(sys.argv[3])
#Inicialmente serian los campos de departamento y municipio
campoAAgrupar1, campoAAgrupar2 = sys.argv[4].split(",")
numArchivosParticionados = int(sys.argv[5])+1
"""
me toco escribir los valores de ciudad y departamento del selectores.py porque pasandolos como parametros en el batch 
no me aceptaba la codificacion, este archivo entonces tiene el departamento y municipio seleccionados
"""

tempCampos = open("tempCamposATrabajar.txt", "r")
#Recibo los valores, por ejemplo QUINDIO Y ARMENIA
departamento, municipio = tempCampos.readline().strip().split(",")
tempCampos.close()
ignorarPrimeraLinea = "" #TIENE el encabezado (agrupaciones o campos)
encabezados = []
resultadosReduccion = []
def sumaResultadosPartes(file, intorfloat, encabezadosAGuardar):
	sumaColumnas = defaultdict(int) #todas las claves se inicializan con valor 0
	with open(file, "r") as mapeosReducciones:
		global ignorarPrimeraLinea #contiene los encabezado
		ignorarPrimeraLinea = mapeosReducciones.readline()
		if encabezadosAGuardar:
			ignorarPrimeraLinea = ignorarPrimeraLinea[:-2] #quito la coma que hay al final y salto de linea, este es el encabezado
			ignorarPrimeraLinea += "\n"
			encabezados.append(ignorarPrimeraLinea)
		for numIter in range(numIteraciones):
			for particion in range(numArchivosParticionados):
				linea = mapeosReducciones.readline()
				linea = linea.strip().split(",")
				for j in range(len(linea)):
					if intorfloat == 1:
						sumaColumnas["campo"+str(j+1)] += int(linea[j])
					else:
						sumaColumnas["campo"+str(j+1)] += float(linea[j])

	sumatoriaTotal = 0 #contiene la suma de todas las columnas
	for suma in sumaColumnas:
		sumaColumnas[suma] = sumaColumnas[suma]/numIteraciones #calculo el promedio, para saber si todos los elementos de las columnas son =s
		sumatoriaTotal += sumaColumnas[suma]
	return sumatoriaTotal

def resultadoReduccionCampos(resultadoReduccion):
	sumaPadre = 0 #contiene el total de la suma con respecto a una categoria, ejemplo, suma de todos los campos de genero
	resultadosMapeoReduccionPorCampo = "" #para cada agrupamiento o campo contiene el resultado de la reduccion
	for doc in resultadoReduccion.find():
		sumaPadre += doc["value"]
		resultadosMapeoReduccionPorCampo += str(doc["value"])+","
	resultadosMapeoReduccionPorCampo = resultadosMapeoReduccionPorCampo[:-1]
	resultadosReduccion.append(resultadosMapeoReduccionPorCampo)
	return sumaPadre

from bson.code import Code
from pymongo import MongoClient

cliente = MongoClient()
db = cliente[baseDatos]
collection = db[archivoPadre]

def mongoMapRedPais(): #es para mostrar los casos por genero en el pais
	mapper = Code("""
	               function () {
	                    emit(this.SEXO, 1);
	               }
	               """)
	reducer = Code("""
	                function (key, values) {
	                  return Array.sum(values);
	                }
	                """)
	return collection.map_reduce(mapper, reducer, "sumaPadre")

def mongoMapRedDepartamento(campoAAgrupar):
	mapper = Code("""
	               function () {
	                    emit(this."""+campoAAgrupar+""", 1);
	               }
	               """)
	reducer = Code("""
	                function (key, values) {
	                  return Array.sum(values);
	                }
	                """)
	return collection.map_reduce(mapper, reducer, "sumaPadre", query = {campoAAgrupar1: departamento})

def mongoMapRedMunicipio(campoAAgrupar):
	mapper = Code("""
	               function () {
	                    emit(this."""+campoAAgrupar+""", 1);
	               }
	               """)
	reducer = Code("""
	                function (key, values) {
	                  return Array.sum(values);
	                }
	                """)
	return collection.map_reduce(mapper, reducer, "sumaPadre", query = {campoAAgrupar1: departamento, campoAAgrupar2: municipio})


sumatoriaTotalCiudadesPython = sumaResultadosPartes("cantidadCasosPorCiudadPython.csv", 1, True)
sumatoriaTotalGenerosPython = sumaResultadosPartes("cantidadCasosPorGeneroDepartamentoPython.csv", 1, True)
sumatoriaTotalGenerosMunicipioPython = sumaResultadosPartes("cantidadCasosPorGenerosMunicipioPython.csv", 1, True)
sumatoriaTotalDiasMunicipioPython = sumaResultadosPartes("cantidadCasosPorDiasMunicipioPython.csv", 1, True)
sumatoriaTotalBarriosMunicipioPython = sumaResultadosPartes("cantidadCasosPorBarriosMunicipioPython.csv", 1, True)
sumatoriaTotalZonasMunicipioPython = sumaResultadosPartes("cantidadCasosPorZonasMunicipioPython.csv", 1, True)
sumatoriaTotalEdadesMunicipioPython = sumaResultadosPartes("cantidadCasosPorEdadesMunicipioPython.csv", 1, True)
sumatoriaTotalEstadosCivilMunicipioPython = sumaResultadosPartes("cantidadCasosPorEstadosCivilMunicipioPython.csv", 1, True)
sumatoriaTotalEscolaridadMunicipioPython = sumaResultadosPartes("cantidadCasosPorEscolaridadMunicipioPython.csv", 1, True)

sumatoriaTotalCiudadesMongo = sumaResultadosPartes("cantidadCasosPorCiudadMongo.csv", 2, False)
sumatoriaTotalGenerosMongo = sumaResultadosPartes("cantidadCasosPorGeneroDepartamentoMongo.csv", 2, False)
sumatoriaTotalGenerosMunicipioMongo = sumaResultadosPartes("cantidadCasosPorGenerosMunicipioMongo.csv", 2, False)
sumatoriaTotalDiasMunicipioMongo = sumaResultadosPartes("cantidadCasosPorDiasMunicipioMongo.csv", 2, False)
sumatoriaTotalBarriosMunicipioMongo = sumaResultadosPartes("cantidadCasosPorBarriosMunicipioMongo.csv", 2, False)
sumatoriaTotalZonasMunicipioMongo = sumaResultadosPartes("cantidadCasosPorZonasMunicipioMongo.csv", 2, False)
sumatoriaTotalEdadesMunicipioMongo = sumaResultadosPartes("cantidadCasosPorEdadesMunicipioMongo.csv", 2, False)
sumatoriaTotalEstadosCivilMunicipioMongo = sumaResultadosPartes("cantidadCasosPorEstadosCivilMunicipioMongo.csv", 2, False)
sumatoriaTotalEscolaridadMunicipioMongo = sumaResultadosPartes("cantidadCasosPorEscolaridadMunicipioMongo.csv", 2, False)

#sumatoriaTotalPartesMongo = sumaResultadosPartes("cantidadCasosPorCiudadMongo.csv", 2)

#******************* POR DEPARTAMENTO
resultadoSumaCiudad = mongoMapRedDepartamento(campoAAgrupar2) #Se pasa en realidad el MUNICIPIO
sumaPadreCiudad = resultadoReduccionCampos(resultadoSumaCiudad)

resultadoSumaGenero = mongoMapRedDepartamento("SEXO")
sumaPadreGenero = resultadoReduccionCampos(resultadoSumaGenero)
#******************** POR MUNICIPIO
resultadoSumaGeneroMunicipio = mongoMapRedMunicipio("SEXO")
sumaPadreGeneroMunicipio = resultadoReduccionCampos(resultadoSumaGeneroMunicipio)

resultadoSumaDiaMunicipio = mongoMapRedMunicipio("DIA")
sumaPadreDiaMunicipio = resultadoReduccionCampos(resultadoSumaDiaMunicipio)

resultadoSumaBarrioMunicipio = mongoMapRedMunicipio("BARRIO")
sumaPadreBarrioMunicipio = resultadoReduccionCampos(resultadoSumaBarrioMunicipio)

resultadoSumaZonaMunicipio = mongoMapRedMunicipio("ZONA")
sumaPadreZonaMunicipio = resultadoReduccionCampos(resultadoSumaZonaMunicipio)

resultadoSumaEdadMunicipio = mongoMapRedMunicipio("EDAD")
sumaPadreEdadMunicipio = resultadoReduccionCampos(resultadoSumaEdadMunicipio)

resultadoSumaEstadoCivilMunicipio = mongoMapRedMunicipio("ESTADOCIVIL")
sumaPadreEstadoCivilMunicipio = resultadoReduccionCampos(resultadoSumaEstadoCivilMunicipio)

resultadoSumaEscolaridadMunicipio = mongoMapRedMunicipio("ESCOLARIDAD")
sumaPadreEscolaridadMunicipio = resultadoReduccionCampos(resultadoSumaEscolaridadMunicipio)

#******************* POR PAIS
resultadoIgnorado = mongoMapRedPais() #Se pasa en realidad el MUNICIPIO
sumaIgnorada = resultadoReduccionCampos(resultadoIgnorado) 

def escribirEncabezadosYResultados(encabezados, resultadosReduccion):
	with open("temporalDeComprobacionParaGraficasMapRed.txt", "a") as temp:#PARA Graficar los resultados
		for i,encabezado in enumerate(encabezados):
			temp.write(encabezado)
			temp.write(resultadosReduccion[i]+"\n")
			if i == len(encabezados) -1:
				temp.write(resultadosReduccion[i+1]+"\n")

escribirEncabezadosYResultados(encabezados, resultadosReduccion) #estos parametros son modificados en resultadoRedccionCampo

print "Resultado del archivo no particionado para Ciudad: "+str(sumaPadreCiudad)
print "Resultado ponderado de los archivos particionados con Python para Ciudad: "+str(sumatoriaTotalCiudadesPython)
print "Resultado ponderado de los archivos particionados con Mongo para Ciudad: "+str(sumatoriaTotalCiudadesMongo)
print "Resultado del archivo no particionado para Genero: "+str(sumaPadreGenero)
print "Resultado ponderado de los archivos particionados con Python para Genero: "+str(sumatoriaTotalGenerosPython)
print "Resultado ponderado de los archivos particionados con Mongo para Genero: "+str(sumatoriaTotalGenerosMongo)
print "Resultado del archivo no particionado para Genero Municipio: "+str(sumaPadreGeneroMunicipio)
print "Resultado ponderado de los archivos particionados con Python para Genero Municipio: "+str(sumatoriaTotalGenerosMunicipioPython)
print "Resultado ponderado de los archivos particionados con Mongo para Genero Municipio: "+str(sumatoriaTotalGenerosMunicipioMongo)
print "Resultado del archivo no particionado para Dia Municipio: "+str(sumaPadreDiaMunicipio)
print "Resultado ponderado de los archivos particionados con Python para Dia Municipio: "+str(sumatoriaTotalDiasMunicipioPython)
print "Resultado ponderado de los archivos particionados con Mongo para Dia Municipio: "+str(sumatoriaTotalDiasMunicipioMongo)
print "Resultado del archivo no particionado para Barrio Municipio: "+str(sumaPadreBarrioMunicipio)
print "Resultado ponderado de los archivos particionados con Python para Barrio Municipio: "+str(sumatoriaTotalBarriosMunicipioPython)
print "Resultado ponderado de los archivos particionados con Mongo para Barrio Municipio: "+str(sumatoriaTotalBarriosMunicipioMongo)
print "Resultado del archivo no particionado para Zona Municipio: "+str(sumaPadreZonaMunicipio)
print "Resultado ponderado de los archivos particionados con Python para Zona Municipio: "+str(sumatoriaTotalZonasMunicipioPython)
print "Resultado ponderado de los archivos particionados con Mongo para Zona Municipio: "+str(sumatoriaTotalZonasMunicipioMongo)
print "Resultado del archivo no particionado para Edad Municipio: "+str(sumaPadreEdadMunicipio)
print "Resultado ponderado de los archivos particionados con Python para Edad Municipio: "+str(sumatoriaTotalEdadesMunicipioPython)
print "Resultado ponderado de los archivos particionados con Mongo para Edad Municipio: "+str(sumatoriaTotalEdadesMunicipioMongo)
print "Resultado del archivo no particionado para Estado Civil Municipio: "+str(sumaPadreEstadoCivilMunicipio)
print "Resultado ponderado de los archivos particionados con Python para Estado Civil Municipio: "+str(sumatoriaTotalEstadosCivilMunicipioPython)
print "Resultado ponderado de los archivos particionados con Mongo para Estado Civil Municipio: "+str(sumatoriaTotalEstadosCivilMunicipioMongo)
print "Resultado del archivo no particionado para Escolaridad Municipio: "+str(sumaPadreEscolaridadMunicipio)
print "Resultado ponderado de los archivos particionados con Python para Escolaridad Municipio: "+str(sumatoriaTotalEscolaridadMunicipioPython)
print "Resultado ponderado de los archivos particionados con Mongo para Escolaridad Municipio: "+str(sumatoriaTotalEscolaridadMunicipioMongo)


#print "Resultado ponderado de los archivos particionados con Mongo: "+str(sumatoriaTotalPartesMongo)