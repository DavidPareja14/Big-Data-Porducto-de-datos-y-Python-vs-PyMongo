#type particiones.csv | python mapPython.py | python reduceyTiemposPython.py arg1 arg2 

import sys
from time import time
from collections import defaultdict

posVectorPosicion = int(sys.argv[1])
longitudVector = int(sys.argv[2])

acumulador = defaultdict(lambda: [])
time_ini = time()
for line in sys.stdin:
	edades, edad = line.strip().split('-')
	if edad == "": #significa que no hay mas lineas y se manda el tiempo que tarda map.
		break
	else:
		acumulador[edades].append(edad)

edadTotal = 0
for entidad in acumulador:
	for ed in acumulador[entidad]:
		edadTotal += int(ed)

time_fin = time()
tiempoTranscurrido = (time_fin - time_ini) + float(edades) #en este caso edasdes tiene el tiempo que tarda map

edTotal = open("resultadosMRPython.txt", "a")
tiempo = open("tiemposMRPython.txt", "a")
if posVectorPosicion ==longitudVector:
	edTotal.write(str(edadTotal)+"\n")
	tiempo.write(str(tiempoTranscurrido)+"\n")
else:
	edTotal.write(str(edadTotal)+",")
	tiempo.write(str(tiempoTranscurrido)+",")

edTotal.close()
tiempo.close()