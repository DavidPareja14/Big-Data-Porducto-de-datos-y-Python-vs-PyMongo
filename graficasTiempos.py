#python graficas.py !x! !numIteraciones!
import matplotlib.pyplot as plt
import sys
from collections import defaultdict

cantidadArchivos = int(sys.argv[1]) #tiene la cantidad de los archivos particionados menos 1
nombreArchivos = []
numeroRegistrosDeTiempos = int(sys.argv[2])
resultadosTiemposPromedio = [] #Por cada lista que tenga, estaran los promedios de todos los tiempos tomados por columna.
cantidadProcesamientos = 2 #el de python y el de mongo. (para pruebas individuales suelo usar un procesamiento)

titulos = ["Procesamiento con Python", "Procesamiento con Mongo"]
coloresLineas = ["Purple", "Cyan"]
"""
Me parece interesante lo que esto hace, se usa numpy para obtener dos columnas,
en cada columna hay 50 numero aleatorios. Luego seran asignados a x e y, los cuales
seran coordenadas para dibujar ya sea un punto rojo o azul.
def plotter (ax, col):
	data = np.random.normal(size = (50, 2))
	x, y = data[:, 0], data[:, 1]
	ax.scatter(x,y,color = col)

fig = plt.figure()
ax = fig.add_axes([0.2, 0.2, 0.7, 0.6])

plotter(ax, "Blue")
plotter(ax, "Red")
plt.show()
"""
#Funcion que dibuja las lineas en la misma grafica.
import matplotlib
def ubicarVentana(f, x, y):
	back = matplotlib.get_backend()
	f.canvas.manager.window.wm_geometry("+%d+%d" %(x,y))

fig1 = plt.figure()
ubicarVentana(fig1, 0, 0)
ax = fig1.add_axes([0.1, 0.1, 0.9, 0.9])
ax.set(xlabel = "Particiones", ylabel = "tiempos")
def dibujarLineasUnaGrafica(ax, nombreDeArchivos, tiemposDeCadaProcesamiento, col, etiquetaLinea):
	ax.plot(nombreDeArchivos, tiemposDeCadaProcesamiento, color = col, label = etiquetaLinea)
	ax.legend()

fig2, ax2 = plt.subplots(cantidadProcesamientos)
ubicarVentana(fig2, 700, 0)
nombreEjesX = ["", "Particiones"]

def dibujarLineasDifGraficas(ax, nombreArchivos, resultadosTiemposPromedio,titulos, col,nombreX, nombreY):
	ax.plot(nombreArchivos, resultadosTiemposPromedio)
	ax.set_title(titulos, color = col)
	ax.set(xlabel = nombreX, ylabel = nombreY)

with open("tiemposMRPython.txt", "r") as parts:
	for _ in range(cantidadArchivos+1): #primero obtengo el nombre de los archivos
		nomReg = parts.readline().strip()
		nomReg = nomReg[:3]+nomReg[8:] #solo mocho el nombre del archivo para que quepan en el eje x sin amontonarse
		nombreArchivos.append(nomReg)
	#como debo obtener los tiempos promedios para el procesamiento en mongo y python, por eso itero 2 veces
	#ya que es el mismo proceso para sacar promedio y graficar.
	for pythonYmongo in range(cantidadProcesamientos):  
		promedios = defaultdict(int)
		for _ in range(numeroRegistrosDeTiempos):
			tiempos = parts.readline().strip().split(",")
			for i in range(len(tiempos)): #cada columna sera como una clave en el diccionario promedios
				promedios["promedio"+str(i)] += float(tiempos[i]) 
		resultadosTiemposPromedio.append([promedios[promedio]/numeroRegistrosDeTiempos for promedio in promedios])
		dibujarLineasUnaGrafica(ax, nombreArchivos, resultadosTiemposPromedio[-1], coloresLineas[pythonYmongo], titulos[pythonYmongo])		
		dibujarLineasDifGraficas(ax2[pythonYmongo], nombreArchivos, resultadosTiemposPromedio[-1], titulos[pythonYmongo], "orange", nombreEjesX[pythonYmongo], "tiempos")

relacionTiemposPythonMongo = zip(resultadosTiemposPromedio[0], resultadosTiemposPromedio[1]) 
fig, speedUp = plt.subplots()
speedUp.set_title("Speed Up (Python vs Mongo)", color = "green")
speedUp.plot(nombreArchivos, [tiempoPython/tiempoMongo for tiempoPython,tiempoMongo in relacionTiemposPythonMongo])

"""mngr = plt.get_current_fig_manager()
mngr.window.setGeometry(50, 100, 640, 545)"""

ubicarVentana(fig, 350, 500)
plt.show()