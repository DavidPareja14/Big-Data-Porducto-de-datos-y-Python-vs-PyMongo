#python selectores.py pruebaFinal Delito_Violencia_Intrafamiliar
import sys
from pymongo import MongoClient
import Tkinter as tk
import ttk
#Es bueno buscar como hacerlo con clases (interfaz), por las limitaciones de internet, ahora no lo pude hacer.
baseDatos = sys.argv[1] #"pruebaFinal"
nombreColeccion = sys.argv[2] #"Delito_Violencia_Intrafamiliar" #esta sera la coleccion con los registros completos (Delitos_Violencia_Intrafamiliar)

cliente = MongoClient()
db = cliente[baseDatos]
collection = db[nombreColeccion]
listaDepartamentos = collection.distinct("DEPARTAMENTO")

departamento = ""
ciudad = ""
listaMunicipios = []
listaGeneros = []

def agregacionConRespectoADepartamento(agrupar, departamento):
	agrupamientoOrdenado = collection.aggregate([
	{"$match" : {"DEPARTAMENTO": departamento}},
	{"$group" : {"_id": "$"+agrupar}},
	{"$sort" : {"_id": 1}} 
	])
	return agrupamientoOrdenado

def agregacionConRespectoAMunicipio(agrupar, departamento, municipio):
	agrupamientoOrdenado = collection.aggregate([
	{"$match" : {"DEPARTAMENTO": departamento, "MUNICIPIO": municipio}},
	{"$group" : {"_id": "$"+agrupar}},
	{"$sort" : {"_id": 1}} 
	])
	return agrupamientoOrdenado

def concatenarInstancias(diccionarioConsulta):
	listaInstancias = list(diccionarioConsulta) 
	concatenacionInstancias = ""
	try:
		for instancia in listaInstancias:
			for ide in instancia:
				concatenacionInstancias += instancia[ide]+","
		concatenacionInstancias += "\n"
	except:
		for instancia in listaInstancias:
			for ide in instancia:
				concatenacionInstancias += str(instancia[ide])+","
		concatenacionInstancias += "\n"
	return concatenacionInstancias

def escribirEncabezados(file, encabezado):
	fileEmpty = open(file, "w")
	fileEmpty.write(encabezado.encode("utf-8")) #se codifica para que acepte caracteres especiales.
	fileEmpty.close()

def seleccionDepartamento(event):
	departamento = comboDepartamentos.get()
	#De acuerdo a mi departamento, obtengo sus munipios y generos en orden alfabetico
	municipios = agregacionConRespectoADepartamento("MUNICIPIO", departamento)

	global listaMunicipios 
	listaMunicipios = list(municipios) 
	temp = [] #tendra los nombres de las ciudades para luego ser asignado al combobox de ciudades
	for municipio in listaMunicipios:
		for ide in municipio:
			temp.append(municipio[ide])
	comboCiudades["values"] = temp
	comboCiudades.set("Seleccione un municipio")

def verificarCampos():
	departamento = comboDepartamentos.get()
	ciudad = comboCiudades.get()
	concatenacionMunicipios = ""
	global listaMunicipios, listaGeneros
	if (ciudad or departamento) == "" or ciudad == "Seleccione un municipio":
		labelAdvertensia = tk.Label(ventanaSelectores, text="Hay campos sin seleccionar", font=("Arial",12), fg="red")
		labelAdvertensia.place(x=20, y=110)
	else:
		for municipio in listaMunicipios:
			for ide in municipio:
				concatenacionMunicipios += municipio[ide]+","
		concatenacionMunicipios += "\n"

		generosDept = agregacionConRespectoADepartamento("SEXO", departamento)
		concatenacionGenerosDepartamento = concatenarInstancias(generosDept)
		dia = agregacionConRespectoAMunicipio("DIA", departamento, ciudad)
		concatenacionDias = concatenarInstancias(dia)
		barrio = agregacionConRespectoAMunicipio("BARRIO", departamento, ciudad)
		concatenacionBarrios = concatenarInstancias(barrio)
		zona = agregacionConRespectoAMunicipio("ZONA", departamento, ciudad)
		concatenacionZonas = concatenarInstancias(zona)
		edad = agregacionConRespectoAMunicipio("EDAD", departamento, ciudad)
		concatenacionEdades = concatenarInstancias(edad)
		generoMunicipio = agregacionConRespectoAMunicipio("SEXO", departamento, ciudad)
		concatenacionGenerosMunicipio = concatenarInstancias(generoMunicipio)
		estadoCivil = agregacionConRespectoAMunicipio("ESTADOCIVIL", departamento, ciudad)
		concatenacionEstadosCivil = concatenarInstancias(estadoCivil)
		escolaridad = agregacionConRespectoAMunicipio("ESCOLARIDAD", departamento, ciudad)
		concatenacionEscolaridad = concatenarInstancias(escolaridad)
		
		escribirEncabezados("cantidadCasosPorCiudadPython.csv", concatenacionMunicipios)
		escribirEncabezados("cantidadCasosPorCiudadMongo.csv", concatenacionMunicipios)
		escribirEncabezados("cantidadCasosPorGeneroDepartamentoPython.csv", concatenacionGenerosDepartamento)
		escribirEncabezados("cantidadCasosPorGeneroDepartamentoMongo.csv", concatenacionGenerosDepartamento)
		escribirEncabezados("cantidadCasosPorDiasMunicipioPython.csv", concatenacionDias)
		escribirEncabezados("cantidadCasosPorDiasMunicipioMongo.csv", concatenacionDias)
		escribirEncabezados("cantidadCasosPorBarriosMunicipioPython.csv", concatenacionBarrios)
		escribirEncabezados("cantidadCasosPorBarriosMunicipioMongo.csv", concatenacionBarrios)
		escribirEncabezados("cantidadCasosPorZonasMunicipioPython.csv", concatenacionZonas)
		escribirEncabezados("cantidadCasosPorZonasMunicipioMongo.csv", concatenacionZonas)
		escribirEncabezados("cantidadCasosPorEdadesMunicipioPython.csv", concatenacionEdades)
		escribirEncabezados("cantidadCasosPorEdadesMunicipioMongo.csv", concatenacionEdades)
		escribirEncabezados("cantidadCasosPorGenerosMunicipioPython.csv", concatenacionGenerosMunicipio)
		escribirEncabezados("cantidadCasosPorGenerosMunicipioMongo.csv", concatenacionGenerosMunicipio)
		escribirEncabezados("cantidadCasosPorEstadosCivilMunicipioPython.csv", concatenacionEstadosCivil)
		escribirEncabezados("cantidadCasosPorEstadosCivilMunicipioMongo.csv", concatenacionEstadosCivil)
		escribirEncabezados("cantidadCasosPorEscolaridadMunicipioPython.csv", concatenacionEscolaridad)
		escribirEncabezados("cantidadCasosPorEscolaridadMunicipioMongo.csv", concatenacionEscolaridad)

		tempCampos = open("tempCamposATrabajar.txt", "w") #contiene valores de municipio y ciudad
		tempCampos.write(departamento.encode("utf-8")+","+ciudad.encode("utf-8")) #se codifica porque esta salida va a un archivo
		tempCampos.close()
		sys.exit(1)

ventanaSelectores = tk.Tk()

ventanaSelectores.title("Datos para dar informacion")
labelBienvenida = tk.Label(ventanaSelectores, text="Bienvenid@, seleccione el departamento y municipio", font=("Arial",15), fg="magenta")
labelBienvenida.place(x=0, y=0)
labelDepartamentos = tk.Label(ventanaSelectores, text="Departamentos: ")
labelDepartamentos.place(x=80, y=50)
comboDepartamentos = ttk.Combobox(state="readonly") #para que el usuario no ingrese valores
comboDepartamentos.place(x = 175, y = 50)
ventanaSelectores.configure(width=470, height=200)
comboCiudades = ttk.Combobox(state="readonly") #para que el usuario no ingrese valores
comboCiudades.place(x = 175, y = 80)
labelCiudades = tk.Label(ventanaSelectores, text="Ciudades: ")
labelCiudades.place(x=90, y=80)
comboDepartamentos["values"] = listaDepartamentos #el combobox solo trabaja con cadenas.
index = comboDepartamentos.current() #obtiene la posicion en la lista
#comboDepartamentos.set("Hola") #Establece el contenido de la lista, ese valor lo muestra en el campo selector.
comboDepartamentos.bind("<<ComboboxSelected>>", seleccionDepartamento) #<<Com..>> es un evento que se llama cuando cambia un valor en la lista
boton = tk.Button(ventanaSelectores, text="Enviar",command=verificarCampos).place(x=200, y=140)

ventanaSelectores.mainloop()
"""
Asi se hace con python3 y clases.
from tkinter import ttk
import tkinter as tk

class Selectores(ttk.Frame):
	def __init__(self, ventanaSelectores):
		super().__init__(ventanaSelectores)
		ventanaSelectores.title("Datos para dar informacion")
		labelDepartamentos = tk.Label(ventanaSelectores, text="Departamentos: ")
		labelDepartamentos.place(x=10, y=50)
		self.comboDepartamentos = ttk.Combobox(self, state="readonly") #para que el usuario no ingrese valores
		self.comboDepartamentos.place(x = 105, y = 50)
		ventanaSelectores.configure(width=300, height=200)
		self.comboCiudades = ttk.Combobox(self, state="readonly") #para que el usuario no ingrese valores
		self.comboCiudades.place(x = 105, y = 80)
		labelCiudades = tk.Label(ventanaSelectores, text="Ciudades: ")
		labelCiudades.place(x=20, y=80)
		self.place(width = 300, height = 200) #Como esta lo anterior, sin esta linea no se pueden ubicar los labels y combobox
		self.comboDepartamentos["values"] = ["van los valores"] #el combobox solo trabaja con cadenas.
		index = self.comboDepartamentos.current() #obtiene la posicion en la lista
		#self.comboDepartamentos.set("Hola") #Establece el contenido de la lista, ese valor lo muestra en el campo selector.
		self.comboDepartamentos.bind("<<ComboboxSelected>>", self.selection_changed) #<<Com..>> es un evento que se llama cuando cambia un valor en la lista

	def selection_changed(self, event):
		print ("Nuevo elemento seleccionado: ", self.comboDepartamentos.get()) #get obtiene el valor seleccionado

ventanaSelectores = tk.Tk()
app = Selectores(ventanaSelectores)
app.mainloop()
"""