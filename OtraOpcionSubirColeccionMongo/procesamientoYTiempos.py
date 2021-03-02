from time import time
import sys

algo = int(sys.argv[1].split("s")[-1]) #debo hacer un script que me permita importar los archivos partidos a mongo
									  #y luego implemento este programa accediendo a las colecciones.
#print algo

time_ini = time()
for _ in range(algo): #aca va el procesamiento con mongo
	pass
time_fin = time()
tiempoTranscurrido = time_fin - time_ini

archivo = open("tiemposMRPython.txt", "a")
if algo == 10:
	archivo.write(str(tiempoTranscurrido)+",")
elif algo == 5:
	archivo.write(str(tiempoTranscurrido)+",")
elif algo == 3:
	archivo.write(str(tiempoTranscurrido)+",")
elif algo == 2:
	archivo.write(str(tiempoTranscurrido)+"\n")

archivo.close()