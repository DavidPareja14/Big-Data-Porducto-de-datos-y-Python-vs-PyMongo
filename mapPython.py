# coding: utf-8
import sys
from time import time

#type particiones.csv | python mapPython.py #esto cambio, ahora es:
#python cargarCamposPython.py args1 arg2  | python mapPython.py numeroCampoaProcesar !valoresDeptYMun!
DIA = 2
BARRIO = 3
ZONA = 4
EDAD = 5
SEXO = 6
ESTADOCIVIL = 7
ESCOLARIDAD = 8

numeroCampos = 0 #Para saber cuantos campos deberia haber en cada registro.
cadenasDeptMun = sys.argv[1].strip().split(",")
#Con estos dos campos puedo hacer las agrupaciones, contiene la posicion (en este caso) del dept y municipio
numeroCampoDepartamento, numeroCampoMunicipio = (int(cadenasDeptMun[0]), int(cadenasDeptMun[1]))
"""
me toco escribir los valores de ciudad y departamento del selectores.py porque pasandolos como parametros en el batch 
no me aceptaba la codificacion, este archivo entonces tiene el departamento y municipio seleccionados
"""
tempCampos = open("tempCamposATrabajar.txt", "r")
#Recibo los valores, por ejemplo QUINDIO Y ARMENIA
departamento, municipio = tempCampos.readline().strip().split(",")
tempCampos.close()

time_ini = time()
#linea es un registro, entonces contiene campos de interes
try:
	for linea in sys.stdin:
		#strip retira saltos de lineaa, tabulaciones, espacios y luego parto la cadena y cada elemento queda en un vector.
		linea = linea.strip().split(",")
		if numeroCampos == 0:
			if linea[-1] == "":
				linea.pop(-1)
			numeroCampos = len(linea) #es la longitud de la primera linea que entre (tiene los nombres de los campos.)
			continue #Creo que no continua con lo de abajo, vuelve hacia el for.		
		linea.pop(-1) #como en cargar campos se anexa una coma al final, con excepcion del encabezado, siempre hay un campo en blanco
		#if len(linea) == numeroCampos: #solo acepto las lineas(registros) que esten completas, hay que tener mucho cuidado con el condicional, esto hacia que no me dieran bien los resultados
		#if len(linea) == numeroCampos: #solo acepto las lineas(registros) que esten completas, hay que tener mucho cuidado con el condicional, esto hacia que no me dieran bien los resultados
		#hay un barrio que solo es un campo asi: "LA ADIELA I,II,III,IV", esto me lo toma como varios campos
		"""campo = linea[numeroCampoDepartamento]
								if campo == departamento: 
									if len(linea) == numeroCampos:
										pass 								#Con esto compruebo lo de la ADIELA
									else:
										print linea"""
		campoDepartamento = linea[numeroCampoDepartamento]
		if campoDepartamento == departamento: 
			campoMunicipio = linea[numeroCampoMunicipio]
			print 'Ciudad*%s**-**%s' % (campoMunicipio, 1)
			print 'GeneroDepartamento*%s**-**%s' % (linea[SEXO], 1)
			if campoMunicipio == municipio:
				print 'GeneroMunicipio*%s**-**%s' % (linea[SEXO], 1)
				print 'DiaMunicipio*%s**-**%s' % (linea[DIA], 1)
				print 'BarrioMunicipio*%s**-**%s' % (linea[BARRIO], 1)
				print 'ZonaMunicipio*%s**-**%s' % (linea[ZONA], 1)
				print 'EdadMunicipio*%s**-**%s' % (linea[EDAD], 1)
				print 'EstadoCivilMunicipio*%s**-**%s' % (linea[ESTADOCIVIL], 1)
				print 'EscolaridadMunicipio*%s**-**%s' % (linea[ESCOLARIDAD], 1)

except:
	pass 
time_fin = time()
tiempoTranscurrido = time_fin - time_ini
print str(tiempoTranscurrido)+"**-**"

#http://python.org/dev/peps/pep-0263/