from time import time
import sys
from pymongo import MongoClient
from bson.code import Code

particion = sys.argv[1] #debo hacer un script que me permita importar los archivoTiemposs partidos a mongo
									  #y luego implemento este programa accediendo a las colecciones.
#variables para saber como escribir en el archivoTiempos tiempos.
posicionEnElVector = int(sys.argv[2])
maximaPosVector = int(sys.argv[3])
baseDatos = sys.argv[4]

cliente = MongoClient()
db = cliente[baseDatos]
collection = db[particion]
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
time_ini = time()
resultadoSuma = collection.map_reduce(mapper, reducer, "suma"+str(posicionEnElVector))
time_fin = time()
tiempoTranscurrido = time_fin - time_ini
#Probar que los resultados dan iguales guardandolos en un archivoSuma y los tiempos en archivoTiempos
archivoTiempos = open("tiemposMRPython.txt", "a")
archivoSuma = open("resultadosMRPythonMongo.txt", "a")
for doc in resultadoSuma.find():
	if posicionEnElVector == maximaPosVector:
		archivoTiempos.write(str(tiempoTranscurrido)+"\n")
		archivoSuma.write(str(doc["value"])+"\n")
	else:
		archivoTiempos.write(str(tiempoTranscurrido)+",")
		archivoSuma.write(str(doc["value"])+",")

archivoTiempos.close()
archivoSuma.close()