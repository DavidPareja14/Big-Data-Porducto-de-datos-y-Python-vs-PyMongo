#python cargarCamposMongo.py baseDatos coleccion campos
import sys
from pymongo import MongoClient

baseDatos = sys.argv[1]
particion = sys.argv[2] #en realidad es la coleccion
seleccionarCampos = sys.argv[3].strip().split(",") #no creo que haya necesidad del strip()
camposDeColeccion = {} #cada campo tendra valor 1 (segun lo que el usuario pase como parametros)
#si solo se pasa un campo, entonces se pasa como (campo,) entonces se crea un campo vacio, esto se verifica
if seleccionarCampos[-1] == "":
	seleccionarCampos.pop(-1)

for campo in seleccionarCampos:
	camposDeColeccion[campo] = 1 #con el valor 1 indico que campos quedaran en las colecciones

cliente = MongoClient()
db = cliente[baseDatos]
col = db[particion]

columnas = [
	{"$project": camposDeColeccion},
	{"$out" : particion}
]

col.aggregate(columnas)