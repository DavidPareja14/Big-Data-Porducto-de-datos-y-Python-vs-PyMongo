#-*- coding= utf-8 -*-
#python graficas.py !x! !numIteraciones!
import matplotlib.pyplot as plt
import matplotlib

datosAGraficar = open("temporalDeComprobacionParaGraficasMapRed.txt", "r")
camposCiudades = datosAGraficar.readline().strip().split(",")
casosCiudades = datosAGraficar.readline().strip().split(",")
camposGenerosDept = datosAGraficar.readline().strip().split(",")
casosGenerosDept = datosAGraficar.readline().strip().split(",")
camposGenerosMun = datosAGraficar.readline().strip().split(",")
casosGenerosMun = datosAGraficar.readline().strip().split(",")
camposDiasMun = datosAGraficar.readline().strip().split(",")
casosDiasMun = datosAGraficar.readline().strip().split(",")
camposBarriosMun = datosAGraficar.readline().strip().split(",")
casosBarriosMun = datosAGraficar.readline().strip().split(",")
camposZonasMun = datosAGraficar.readline().strip().split(",")
casosZonasMun = datosAGraficar.readline().strip().split(",")
camposEdadesMun = datosAGraficar.readline().strip().split(",")
casosEdadesMun = datosAGraficar.readline().strip().split(",")
camposEstadosCivilMun = datosAGraficar.readline().strip().split(",")
casosEstadosCivilMun = datosAGraficar.readline().strip().split(",")
camposEscolaridadMun = datosAGraficar.readline().strip().split(",")
casosEscolaridadMun = datosAGraficar.readline().strip().split(",")
camposSexoPais = ["", "FEMENINO", "MASCULINO", "NO REPORTADO"] #como es por pais, esto es constante, solo hay un pais, Colombia.
casosSexoPais = datosAGraficar.readline().strip().split(",")


camposCiudades = [unicode(x, "utf-8") for x in camposCiudades]
casosCiudades = [float(valor) for valor in casosCiudades]
casosGenerosDept = [float(valor) for valor in casosGenerosDept]
casosGenerosMun = [float(valor) for valor in casosGenerosMun]
camposDiasMun = [unicode(x, "utf-8") for x in camposDiasMun]
casosDiasMun = [float(valor) for valor in casosDiasMun]
camposBarriosMun = [unicode(x, "utf-8") for x in camposBarriosMun]
casosBarriosMun = [float(valor) for valor in casosBarriosMun]
casosZonasMun = [float(valor) for valor in casosZonasMun]
casosEdadesMun = [float(valor) for valor in casosEdadesMun]
casosEstadosCivilMun = [float(valor) for valor in casosEstadosCivilMun]
casosEscolaridadMun = [float(valor) for valor in casosEscolaridadMun]
casosSexoPais = [float(valor) for valor in casosSexoPais]

datosAGraficar.close()

def ordenarDiagramaCircular(lista, listaNom):
	tempNombre = ""
	tempElem = 0
	menoresA = 5
	for i,elem in enumerate(lista):
		if elem < menoresA:
			if i == 0 and lista[i+1]<menoresA:
				for j in range(i+2,len(lista)-2):
					if lista[j] >= menoresA  and lista[j+2] >= menoresA:
						tempElem = lista[j+1]
						lista[j+1] = elem
						lista[i] = tempElem
						tempNombre = listaNom[j+1]
						listaNom[j+1] = listaNom[i]
						listaNom[i] = tempNombre
						break
			elif i == len(lista)-1:
				break
			else:
				if lista[i-1]<menoresA  and lista[i+1]<menoresA :
					for j in range(len(lista)-1):
						if lista[j] >= menoresA and lista[j+1] >= menoresA:
							tempElem = lista[j]
							lista[j] = elem
							lista[i] = tempElem
							tempNombre = listaNom[j]
							listaNom[j] = listaNom[i]
							listaNom[i] = tempNombre
							break
				elif lista[i-1]<menoresA:
					tempElem = lista[i+1]
					lista[i+1] = elem
					lista[i] = tempElem
					tempNombre = listaNom[i+1]
					listaNom[i+1] = listaNom[i]
					listaNom[i] = tempNombre
				elif lista[i+1]<menoresA:
					tempElem = lista[i-1]
					lista[i-1] = elem
					lista[i] = tempElem
					tempNombre = listaNom[i-1]
					listaNom[i-1] = listaNom[i]
					listaNom[i] = tempNombre
	return (listaNom, lista)

def ubicarVentana(f, x, y):
	back = matplotlib.get_backend()
	f.canvas.manager.window.wm_geometry("+%d+%d" %(x,y))

def diagramaDeBarras(numCasos, campos, titulo, posVentanaX, posVentanaY):
	fig = plt.figure(titulo, figsize=(2, 2))
	ax = fig.add_axes([0.1, 0.15, 0.8, 0.8])
	cantidad = range(len(numCasos))

	ax.bar(cantidad, numCasos, width=0.95, align="center")
	plt.xticks(fontsize=5) #camtio el tamano de los nombres en los ejes para que quepa todo
	ax.set_xticks(cantidad)
	ax.set_xticklabels(campos, rotation=80)
	ubicarVentana(fig, posVentanaX, posVentanaY)

fig1 = plt.figure("Casos Genero por Departamento y Municipio",figsize=(2, 2))
ax = fig1.add_axes([0.1, 0.1, 0.9, 0.9])
def diagramaLineal(generos, cantidadCasos, col, etiquetaLinea):
	ax.set(xlabel = "Cantidad Casos", ylabel = "Genero")
	ax.plot(generos, cantidadCasos, color = col, label = etiquetaLinea)
	ax.legend()
	ubicarVentana(fig1, 380,0)

def mayorLista(lista):
	mayor = 0
	posMayor = 0
	for pos,elem in enumerate(lista):
		if elem>mayor:
			mayor = elem
			posMayor = pos
	return posMayor

def diagramaCircular(dias, numeroCasos, titulo, posVentanaX, posVentanaY): #Pie Chart
	labels, sizes = ordenarDiagramaCircular(numeroCasos, dias)
	pos = mayorLista(numeroCasos)
	explode = [(x==pos)*0.1 for x in range(len(numeroCasos))] #Se amplia la porcion en el grafico para el valor diferente de 0

	fig2, ax1 = plt.subplots(figsize=(2, 2))
	fig2.suptitle(titulo, color = "orange")
	ax1.pie(sizes, explode = explode, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)
	ax1.axis("equal")
	ubicarVentana(fig2, posVentanaX, posVentanaY)


diagramaCircular(camposEscolaridadMun, casosEscolaridadMun,"Casos Por Nivel de Educacio en el municipio",1030,350)
diagramaCircular(camposEstadosCivilMun, casosEstadosCivilMun,"Casos Por Estado Civil en el municipio",730,350)
diagramaDeBarras(casosEdadesMun,camposEdadesMun,"Cantidad de casos por Edad en el Municipio",380,350)
diagramaCircular(camposZonasMun, casosZonasMun,"Casos Por Zona en el municipio",30,350)
diagramaDeBarras(casosBarriosMun,camposBarriosMun,"Cantidad de casos por Barrio en el Municipio",1030,0)
diagramaCircular(camposDiasMun, casosDiasMun,"Casos Por Dia en el municipio",730,0)
diagramaLineal(camposSexoPais,casosSexoPais,"purple","Cantidad de casos por Genero en el Pais")
diagramaLineal(camposGenerosDept,casosGenerosDept,"green","Cantidad de casos por Genero en el Departamento")
diagramaLineal(camposGenerosMun,casosGenerosMun,"red","Cantidad de casos por Genero en el Municipio")
diagramaDeBarras(casosCiudades,camposCiudades,"Cantidad de casos por ciudad Departamento",30,0)

plt.show()