#python comprobacionResultadosProcesamiento.py pruebaFinal datosPrueba cantidadLineas

from collections import defaultdict
import sys

baseDatos = sys.argv[1] #el nombre de la base de datos donde subi las particiones y el padre.
archivoPadre = sys.argv[2] #el archivo del cual creo las particiones (sin extension).

numeorLineas = int(sys.argv[3])

def sumaResultadosPartes(file, intorfloat):
	sumaColumnas = defaultdict(int) #todas las claves se inicializan con valor 0
	with open(file, "r") as sumas:
		for _ in range(numeorLineas): #10 porque es la cantidad de lineas del archivo
			linea = sumas.readline()
			linea = linea.strip().split(",")
			for j in range(len(linea)):
				if intorfloat == 1:
					sumaColumnas["columna"+str(j+1)] += int(linea[j])
				else:
					sumaColumnas["columna"+str(j+1)] += float(linea[j])

	sumatoriaTotal = 0 #contiene la suma de todas las columnas
	for suma in sumaColumnas:
		sumaColumnas[suma] = sumaColumnas[suma]/numeorLineas #calculo el promedio, para saber si todos los elementos de las columnas son =s
		sumatoriaTotal += sumaColumnas[suma]
	return sumatoriaTotal

from pymongo import MongoClient
from bson.code import Code

cliente = MongoClient()
db = cliente[baseDatos]
collection = db[archivoPadre]
mapper = Code("""
               function () {
                   emit("sumaEdades", this.edad);
               }
               """)
reducer = Code("""
                function (key, values) {
                  return Array.sum(values);
                }
                """)
resultadoSuma = collection.map_reduce(mapper, reducer, "sumaPadre")

for doc in resultadoSuma.find():
	sumaPadre = doc["value"]

sumatoriaTotalPartesPython = sumaResultadosPartes("resultadosMRPython.txt", 1)
sumatoriaTotalPartesMongo = sumaResultadosPartes("resultadosMRPythonMongo.txt", 2)

print "Resultado del archivo no particionado: "+str(sumaPadre)
print "Resultado ponderado de los archivos particionados con Python: "+str(sumatoriaTotalPartesPython)
print "Resultado ponderado de los archivos particionados con Mongo: "+str(sumatoriaTotalPartesMongo)