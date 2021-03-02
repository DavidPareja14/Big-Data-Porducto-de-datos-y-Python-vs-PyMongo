import sys
from time import time

#type particiones.csv | python mapPython.py

numeroCampos = 0 #Para saber cuantos campos deberia haber en cada registro.
time_ini = time()
try:
	for linea in sys.stdin:
		#strip retira saltos de lineaa, tabulaciones, espacios y luego parto la cadena y cada elemento queda en un vector.
		linea = linea.strip().split(",")
		if numeroCampos == 0:
			numeroCampos = len(linea) #es la longitud de la primera linea que entre (tiene los nombres de los campos.)
		elif len(linea) == numeroCampos:
			edad = linea[1]
			print '%s-%s' % ("edades", edad)

except:
	pass 
time_fin = time()
tiempoTranscurrido = time_fin - time_ini
print str(tiempoTranscurrido)+"-"