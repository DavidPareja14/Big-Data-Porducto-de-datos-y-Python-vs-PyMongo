#python cargarCamposPython.py datosPrueba.csv edad,...
import sys
import csv

file = sys.argv[1] #es un archivo particionado
campos = sys.argv[2] #recibo los campos separados por comas.
concatenacionCampos = ""

print campos #Primero se manda el nombre de los encabezados para saber que longitud debe tener cada registro
campos = campos.strip().split(",")
if campos[-1] == "": #si solo se trabaja con un campo (edad,) el ultimo campos sera vacio
	campos.pop(-1)

archivoCsv = open(file, "r")
lector = csv.DictReader(archivoCsv)
for doc in lector:
	for campo in campos:
		concatenacionCampos += doc[campo]+","
	#concatenacionCampos = concatenacionCampos[1:] #la primera coma no la asigno
	print concatenacionCampos
	concatenacionCampos = ""