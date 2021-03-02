#python cargarCamposPython.py args1 arg2  | python mapPython.py numeroCampoaProcesar | python reduceyTiemposPython.py arg1 arg2 
#python cargarCamposPython.py !filePart! !campos! | python mapPython.py !campoATrabajarPython! | python reduceyTiemposPython.py %%n !x!
import sys
from time import time
from collections import defaultdict

#args utililes para escribir bien los resultados en el archivo
posVectorPosicion = int(sys.argv[1])
longitudVector = int(sys.argv[2])

def obtenerEncabezadosDeInstancias(file):
	ciudadesOrdenadas = open(file, "r") #selectores.py pone la primer linea con las ciudades
	encabezado = ciudadesOrdenadas.readline().strip().split(",") #lista con todas las ciudades, primero linea
	ciudadesOrdenadas.close()
	encabezado.pop(-1) #en la ultima pos debe haber un espacio en blanco
	return encabezado

ciudades = obtenerEncabezadosDeInstancias("cantidadCasosPorCiudadPython.csv")
generos = obtenerEncabezadosDeInstancias("cantidadCasosPorGeneroDepartamentoPython.csv")
generosMunicipio = obtenerEncabezadosDeInstancias("cantidadCasosPorGenerosMunicipioPython.csv")
diasMunicipio = obtenerEncabezadosDeInstancias("cantidadCasosPorDiasMunicipioPython.csv")
barriosMunicipio = obtenerEncabezadosDeInstancias("cantidadCasosPorBarriosMunicipioPython.csv")
zonasMunicipio = obtenerEncabezadosDeInstancias("cantidadCasosPorZonasMunicipioPython.csv")
edadesMunicipio = obtenerEncabezadosDeInstancias("cantidadCasosPorEdadesMunicipioPython.csv")
estadosCivilMunicipio = obtenerEncabezadosDeInstancias("cantidadCasosPorEstadosCivilMunicipioPython.csv")
escolaridadMunicipio = obtenerEncabezadosDeInstancias("cantidadCasosPorEscolaridadMunicipioPython.csv")

acumulador = defaultdict(lambda: []) #tendra las agrupaciones
cantidadCasosInstanciaCategoria = 0
#Esta lista es para comparar con la lista ciudades, si el orden no es el mismo, se deberan agregar ceros,
#ya que se supone que el archivo particionado no contiene la ciudad
listaCiudadesCantidad = []
listaGeneroDepartamentoCantidad = []
listaGeneroMunicipioCantidad = []
listaDiaMunicipioCantidad = []
listaBarrioMunicipioCantidad = []
listaZonaMunicipioCantidad = []
listaEdadMunicipioCantidad = []
listaEstadoCivilMunicipioCantidad = []
listaEscolaridadMunicipioCantidad = []

time_ini = time()
for line in sys.stdin:
	campoAagrupar, valor = line.strip().split('**-**')
	if valor == "": #significa que no hay mas lineas y se manda el tiempo que tarda map.
		break
	else:
		acumulador[campoAagrupar].append(valor) 

for entidad in acumulador:
	categoria, instanciaCategoria = entidad.split("*")
	for val in acumulador[entidad]:
		cantidadCasosInstanciaCategoria += int(val) #ESTOY  haciendo la reduccion
	if categoria == "Ciudad":
		listaCiudadesCantidad.append(instanciaCategoria+"-,-"+str(cantidadCasosInstanciaCategoria))
	elif categoria == "GeneroDepartamento":
		listaGeneroDepartamentoCantidad.append(instanciaCategoria+"-,-"+str(cantidadCasosInstanciaCategoria))
	elif categoria == "GeneroMunicipio":
		listaGeneroMunicipioCantidad.append(instanciaCategoria+"-,-"+str(cantidadCasosInstanciaCategoria))
	elif categoria == "DiaMunicipio":
		listaDiaMunicipioCantidad.append(instanciaCategoria+"-,-"+str(cantidadCasosInstanciaCategoria))
	elif categoria == "BarrioMunicipio":
		listaBarrioMunicipioCantidad.append(instanciaCategoria+"-,-"+str(cantidadCasosInstanciaCategoria))
	elif categoria == "ZonaMunicipio":
		listaZonaMunicipioCantidad.append(instanciaCategoria+"-,-"+str(cantidadCasosInstanciaCategoria))
	elif categoria == "EdadMunicipio":
		listaEdadMunicipioCantidad.append(instanciaCategoria+"-,-"+str(cantidadCasosInstanciaCategoria))
	elif categoria == "EstadoCivilMunicipio":
		listaEstadoCivilMunicipioCantidad.append(instanciaCategoria+"-,-"+str(cantidadCasosInstanciaCategoria))
	elif categoria == "EscolaridadMunicipio":
		listaEscolaridadMunicipioCantidad.append(instanciaCategoria+"-,-"+str(cantidadCasosInstanciaCategoria))
	cantidadCasosInstanciaCategoria = 0

time_fin = time()
tiempoTranscurrido = (time_fin - time_ini) + float(campoAagrupar) #en este caso edasdes tiene el tiempo que tarda map

def datosReducidosAEscribir(listaInstanciaValor, encabezadoInstancias, siEsPorEdadOBarrio):
	if siEsPorEdadOBarrio:
		listaInstanciaValor.sort() #Se ordena alfabeticamente
		#los numeros me los ordena alfabeticamente y no numericamente, como son strings, no puedo solamente
		#ordenar listaInstanciaValor, Ya que por defecto, encabezadoInstancias siempre estara bien ordenado,
		#toca ordenar entonces con sort().
		encabezadoInstancias.sort() 
	else:
		listaInstanciaValor.sort() #Se ordena alfabeticamente
	#voy a tratar de poner 0 donde no coincida en nombre de la ciudad o instancia, pero debo guardar cantidadCasos hasta
	#que haya coincidencia para asignarlo
	numeroCasosAEscribir = ""
	#Este condicional es porque almenos cuando se agrupa por departamento quindio, se obtienen mas campos que con
  	#el mapeo y reduccion de mongo, el segundo archivo particionado mas grande es el que genera los agrupamientos adicionales
  	#entonces procedo a eliminarlos. Se genera al menos en Armenia, campos adicionales, porque hay barrios como la 
  	#Adiela 1,2,3 y 4 que estan separados por comas, esto hace que el readDict de cargarCamposPython los tome como campos diferentes.
	temp = []
	#print listaInstanciaValor
	#print encabezadoInstancias
	if len(listaInstanciaValor) > len(encabezadoInstancias):
		for nombre in encabezadoInstancias:
			for elem in listaInstanciaValor:
				key, value = elem.split("-,-")
				if key==nombre:
					temp.append(elem)
					break
		listaInstanciaValor = temp
	if len(listaInstanciaValor) != 0: #pueden que no hayan ciudades o instancia en un archivo para un departamento.
		aunNoAsignado = 0
		for i,ciudad in enumerate(encabezadoInstancias):
			nombreCiudad, cantidadCasos = listaInstanciaValor[aunNoAsignado].split("-,-")
			if ciudad == nombreCiudad:
				if aunNoAsignado+1 < len(listaInstanciaValor):
					aunNoAsignado += 1
				numeroCasosAEscribir += cantidadCasos+","
			else:
				numeroCasosAEscribir += "0"+","
		numeroCasosAEscribir = numeroCasosAEscribir[:-1] #para quitar la ultima , 
	else:
		for _ in range(len(encabezadoInstancias)):
			numeroCasosAEscribir += "0"+"," #lleno toda una fila de ceros
		numeroCasosAEscribir = numeroCasosAEscribir[:-1] #para quitar la ultima , 
	return numeroCasosAEscribir
#print listaGeneroDepartamentoCantidad
camposReducidosCiudad = datosReducidosAEscribir(listaCiudadesCantidad, ciudades, False)
camposReducidosGenero = datosReducidosAEscribir(listaGeneroDepartamentoCantidad, generos, False)
camposReducidosGeneroMunicipio = datosReducidosAEscribir(listaGeneroMunicipioCantidad, generosMunicipio, False)
camposReducidosDiaMunicipio = datosReducidosAEscribir(listaDiaMunicipioCantidad, diasMunicipio, False)
camposReducidosBarrioMunicipio = datosReducidosAEscribir(listaBarrioMunicipioCantidad, barriosMunicipio, True) #Hay barrios que empiezan con numero
camposReducidosZonaMunicipio = datosReducidosAEscribir(listaZonaMunicipioCantidad, zonasMunicipio, False)
camposReducidosEdadMunicipio = datosReducidosAEscribir(listaEdadMunicipioCantidad, edadesMunicipio, True)
camposReducidosEstadoCivilMunicipio = datosReducidosAEscribir(listaEstadoCivilMunicipioCantidad, estadosCivilMunicipio, False)
camposReducidosEscolaridadMunicipio = datosReducidosAEscribir(listaEscolaridadMunicipioCantidad, escolaridadMunicipio, False)

def escribirResultadosReduccion(file, resultados):
	archivo = open(file, "a")
	archivo.write(resultados+"\n")
	archivo.close()

escribirResultadosReduccion("cantidadCasosPorCiudadPython.csv", camposReducidosCiudad)
escribirResultadosReduccion("cantidadCasosPorGeneroDepartamentoPython.csv", camposReducidosGenero)
escribirResultadosReduccion("cantidadCasosPorGenerosMunicipioPython.csv", camposReducidosGeneroMunicipio)
escribirResultadosReduccion("cantidadCasosPorDiasMunicipioPython.csv", camposReducidosDiaMunicipio)
escribirResultadosReduccion("cantidadCasosPorBarriosMunicipioPython.csv", camposReducidosBarrioMunicipio)
escribirResultadosReduccion("cantidadCasosPorZonasMunicipioPython.csv", camposReducidosZonaMunicipio)
escribirResultadosReduccion("cantidadCasosPorEdadesMunicipioPython.csv", camposReducidosEdadMunicipio)
escribirResultadosReduccion("cantidadCasosPorEstadosCivilMunicipioPython.csv", camposReducidosEstadoCivilMunicipio)
escribirResultadosReduccion("cantidadCasosPorEscolaridadMunicipioPython.csv", camposReducidosEscolaridadMunicipio)

tiempo = open("tiemposMRPython.txt", "a")

if posVectorPosicion == longitudVector:
	tiempo.write(str(tiempoTranscurrido)+"\n")
else:
	tiempo.write(str(tiempoTranscurrido)+",")

tiempo.close()
"""
Antes, como de cada archivo se calculaba la suma total del campo edad, se escribia de esta manera, con esto
detectaba cual era el archivo final que se procesaba y escribia su valor de la edad con un salto de linea
edTotal = open("resultadosMRPython.txt", "a")
tiempo = open("tiemposMRPython.txt", "a")
if posVectorPosicion ==longitudVector:
	edTotal.write(str(cantidadCasosCiudad)+"\n")
	tiempo.write(str(tiempoTranscurrido)+"\n")
else:
	edTotal.write(str(cantidadCasosCiudad)+",")
	tiempo.write(str(tiempoTranscurrido)+",")

edTotal.close()
tiempo.close()"""