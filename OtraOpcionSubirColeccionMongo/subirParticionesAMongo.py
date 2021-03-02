import sys
import csv
from pymongo import MongoClient

nombres = sys.stdin.readline() #sys.stdin es de tipo file, solo necesito obtener los nombres de los archivos particionados.
listaNombres = nombres.split(",")
#print listaNombres
for pos, nombre in enumerate(listaNombres):
	listaNombres[pos] = listaNombres[pos].split("'")[1]
	print listaNombres[pos]

cliente = MongoClient()
db = cliente.pruebaFinalPrueba #Nombre de la base de datos.
"""col = db["10Registros"]
col.drop()
col = db["5Registros"]
col.drop()
col = db["3Registros"]
col.drop()
col = db["2Registros"]
col.drop()"""
archivoCsv = open("datosPrueba.csv", "r")
lector = csv.DictReader(archivoCsv)
collection = db["datosPrueba"]
collection.drop()
encabezado = ["nombre", "edad", "anio"]
for each in lector:
	row = {}
	for campo in encabezado:
		row[campo] = each[campo]
	collection.insert(row)

"""archivoCsv = open("prod_cacao_dpt.csv", "r")
lector = csv.DictReader(archivoCsv) #al parecer todos los campos me los pone como cadenas, hasta lo numericos.
collection = db["prod_cacao_dpt"]
collection.drop()
for each in lector:
	collection.insert(each)"""
for i in range(len(listaNombres)):		
	archivoCsv = open(listaNombres[i]+".csv", "r")
	lector = csv.DictReader(archivoCsv)
	collection = db[listaNombres[i]]
	collection.drop()

	for each in lector:
		collection.insert(each)

col = db["Registros10"]
col.drop()
col = db["Registros5"]
col.drop()
col = db["Registros3"]
col.drop()
col = db["Registros2"]
col.drop()
collection = db["datosPrueba"]
collection.drop()