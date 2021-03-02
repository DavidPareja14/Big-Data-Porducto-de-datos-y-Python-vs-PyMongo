#python graficas.py !x! !numIteraciones!
import matplotlib.pyplot as plt
import sys
from collections import defaultdict

cantidadArchivos = int(sys.argv[1]) #tiene la cantidad de los archivos particionados menos 1
nombreArchivos = []
numeroRegistrosDeTiempos = int(sys.argv[2])
resultadosTiemposPromedio = [] #Por cada lista que tenga, estaran los promedios de todos los tiempos tomados por columna.
cantidadProcesamientos = 2 #el de python y el de mongo.
fig, axs = plt.subplots(cantidadProcesamientos)
titulos = ["Tiempos Procesamiento con Python", "Tiempos Procesamiento con Mongo"]
nombreEjesX = ["", "Particiones"]

with open("tiemposMRPython.txt", "r") as parts:
	for _ in range(cantidadArchivos+1): #primero obtengo el nombre de los archivos
		nombreArchivos.append(parts.readline().strip())
	#como debo obtener los tiempos promedios para el procesamiento en mongo y python, por eso itero 2 veces
	#ya que es el mismo proceso para sacar promedio y graficar.
	for pythonYmongo in range(cantidadProcesamientos):  
		promedios = defaultdict(int)
		for _ in range(numeroRegistrosDeTiempos):
			tiempos = parts.readline().strip().split(",")
			for i in range(len(tiempos)): #cada columna sera como una clave en el diccionario promedios
				promedios["promedio"+str(i)] += float(tiempos[i]) 
		resultadosTiemposPromedio.append([promedios[promedio]/numeroRegistrosDeTiempos for promedio in promedios])
		axs[pythonYmongo].plot(nombreArchivos, resultadosTiemposPromedio[-1])
		axs[pythonYmongo].set_title(titulos[pythonYmongo], color="orange")
		axs[pythonYmongo].set(xlabel = nombreEjesX[pythonYmongo], ylabel = "tiempos")

relacionTiemposPythonMongo = zip(resultadosTiemposPromedio[0], resultadosTiemposPromedio[1]) 
fig, speedUp = plt.subplots()
speedUp.set_title("Speed Up", color = "green")
speedUp.plot(nombreArchivos, [tiempoPython/tiempoMongo for tiempoPython,tiempoMongo in relacionTiemposPythonMongo])
plt.show()