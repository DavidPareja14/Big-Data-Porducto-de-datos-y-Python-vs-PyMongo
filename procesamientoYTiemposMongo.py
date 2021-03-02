# coding: utf-8
#python procesamientoYTiemposMongo.py !filesNames[%%n]! %%n !x! !baseDatos! !campoATrabajarMongo!
from time import time
import sys
from pymongo import MongoClient
from bson.code import Code

particion = sys.argv[1] #debo hacer un script que me permita importar los archivoTiemposs partidos a mongo
									  #y luego implemento este programa accediendo a las colecciones.
#variables para saber como escribir en el archivoTiempos tiempos.
posicionEnElVector = int(sys.argv[2])
maximaPosVector = int(sys.argv[3])
baseDatos = sys.argv[4]
campoAAgrupar1, campoAAgrupar2 = sys.argv[5].split(",")

tempCampos = open("tempCamposATrabajar.txt", "r")
#Recibo los valores, por ejemplo QUINDIO Y ARMENIA
departamento, municipio = tempCampos.readline().strip().split(",")
tempCampos.close()
#Estas listas van a contener el valor del campo junto con su identificador, separados por -,-, esto es fundamenta,
#para poder escribir bien los resultados en los archivos. Se usan, porque seguramente como se trabaja con los archivos
#particionados, hay archivos pequenos que no contienen cierto valor agrupado, entonces en ese valor se debe poner 0,
#esto se hace con la funcion datosReducidosAEscribir
listaCiudadesCantidad = []
listaGeneroDepartamentoCantidad = []
listaGeneroMunicipioCantidad = []
listaDiaMunicipioCantidad = []
listaBarrioMunicipioCantidad = []
listaZonaMunicipioCantidad = []
listaEdadMunicipioCantidad = []
listaEstadoCivilMunicipioCantidad = []
listaEscolaridadMunicipioCantidad = []

def obtenerEncabezadosDeInstancias(file):
  ciudadesOrdenadas = open(file, "r") #selectores.py pone la primer linea con las ciudades
  encabezado = ciudadesOrdenadas.readline().strip().split(",") #lista con todas las ciudades, primero linea
  ciudadesOrdenadas.close()
  encabezado.pop(-1) #en la ultima pos debe haber un espacio en blanco
  return encabezado

ciudades = obtenerEncabezadosDeInstancias("cantidadCasosPorCiudadMongo.csv")
generos = obtenerEncabezadosDeInstancias("cantidadCasosPorGeneroDepartamentoMongo.csv")
generosMunicipio = obtenerEncabezadosDeInstancias("cantidadCasosPorGenerosMunicipioMongo.csv")
diasMunicipio = obtenerEncabezadosDeInstancias("cantidadCasosPorDiasMunicipioMongo.csv")
barriosMunicipio = obtenerEncabezadosDeInstancias("cantidadCasosPorBarriosMunicipioMongo.csv")
zonasMunicipio = obtenerEncabezadosDeInstancias("cantidadCasosPorZonasMunicipioMongo.csv")
edadesMunicipio = obtenerEncabezadosDeInstancias("cantidadCasosPorEdadesMunicipioMongo.csv")
estadosCivilMunicipio = obtenerEncabezadosDeInstancias("cantidadCasosPorEstadosCivilMunicipioMongo.csv")
escolaridadMunicipio = obtenerEncabezadosDeInstancias("cantidadCasosPorEscolaridadMunicipioMongo.csv")

cliente = MongoClient()
db = cliente[baseDatos]
collection = db[particion]
def mongoMapRedDepartamento(campoAAgrupar):
  mapper = Code("""
                 function () {
                      emit(this."""+campoAAgrupar+""", 1);
                 }
                 """)
  reducer = Code("""
                  function (key, values) {
                    return Array.sum(values);
                  }
                  """)
  return collection.map_reduce(mapper, reducer, "sumaPadre", query = {campoAAgrupar1: departamento})

def mongoMapRedMunicipio(campoAAgrupar):
  mapper = Code("""
                 function () {
                      emit(this."""+campoAAgrupar+""", 1);
                 }
                 """)
  reducer = Code("""
                  function (key, values) {
                    return Array.sum(values);
                  }
                  """)
  return collection.map_reduce(mapper, reducer, "sumaPadre", query = {campoAAgrupar1: departamento, campoAAgrupar2: municipio})

def resultadoReduccionCampos(resultadoReduccion, listaCategoria, campoEntero):
  for doc in resultadoReduccion.find():
    if campoEntero:
      listaCategoria.append(str(doc["_id"])+"-,-"+str(doc["value"]))
    else:
      try:
        listaCategoria.append(doc["_id"]+"-,-"+str(doc["value"]))
      except:
        listaCategoria.append(doc["_id"].encode('ascii', 'ignore')+"-,-"+str(doc["value"]))

def datosReducidosAEscribir(listaInstanciaValor, encabezadoInstancias, siEsPorEdadOBarrio, valoresFlotantes):
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
  #print listaInstanciaValor
  #print encabezadoInstancias
  if len(listaInstanciaValor) != 0: #pueden que no hayan ciudades o instancia en un archivo para un departamento.
    aunNoAsignado = 0
    for i,ciudad in enumerate(encabezadoInstancias):
      nombreCiudad, cantidadCasos = listaInstanciaValor[aunNoAsignado].split("-,-")
      if valoresFlotantes and ciudad != "NO REPORTADO" and ciudad != "NO REPORTADO1" and ciudad != "":
        ciudad = float(ciudad) #en el documento el encabezado de edades esta en int, mongo los trabaja como float
        if nombreCiudad != "NO REPORTADO" and nombreCiudad != "NO REPORTADO1" and nombreCiudad != "":
          nombreCiudad = float(nombreCiudad)
      else:
        nombreCiudad = nombreCiudad.encode("utf-8")
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

time_ini = time()

resultadoSumaCiudad = mongoMapRedDepartamento(campoAAgrupar2) #Se pasa en realidad el MUNICIPIO
resultadoReduccionCampos(resultadoSumaCiudad, listaCiudadesCantidad, False)

resultadoSumaGenero = mongoMapRedDepartamento("SEXO")
resultadoReduccionCampos(resultadoSumaGenero, listaGeneroDepartamentoCantidad, False)
#******************** POR MUNICIPIO
resultadoSumaGeneroMunicipio = mongoMapRedMunicipio("SEXO")
resultadoReduccionCampos(resultadoSumaGeneroMunicipio, listaGeneroMunicipioCantidad, False)

resultadoSumaDiaMunicipio = mongoMapRedMunicipio("DIA")
resultadoReduccionCampos(resultadoSumaDiaMunicipio, listaDiaMunicipioCantidad, False)

resultadoSumaBarrioMunicipio = mongoMapRedMunicipio("BARRIO")
resultadoReduccionCampos(resultadoSumaBarrioMunicipio, listaBarrioMunicipioCantidad, False)

resultadoSumaZonaMunicipio = mongoMapRedMunicipio("ZONA")
resultadoReduccionCampos(resultadoSumaZonaMunicipio, listaZonaMunicipioCantidad, False)

resultadoSumaEdadMunicipio = mongoMapRedMunicipio("EDAD")
resultadoReduccionCampos(resultadoSumaEdadMunicipio, listaEdadMunicipioCantidad, True)

resultadoSumaEstadoCivilMunicipio = mongoMapRedMunicipio("ESTADOCIVIL")
resultadoReduccionCampos(resultadoSumaEstadoCivilMunicipio, listaEstadoCivilMunicipioCantidad, False)

resultadoSumaEscolaridadMunicipio = mongoMapRedMunicipio("ESCOLARIDAD")
resultadoReduccionCampos(resultadoSumaEscolaridadMunicipio, listaEscolaridadMunicipioCantidad, False)

time_fin = time()
tiempoTranscurrido = time_fin - time_ini

camposReducidosCiudad = datosReducidosAEscribir(listaCiudadesCantidad, ciudades, False, False)
camposReducidosGenero = datosReducidosAEscribir(listaGeneroDepartamentoCantidad, generos, False, False)
camposReducidosGeneroMunicipio = datosReducidosAEscribir(listaGeneroMunicipioCantidad, generosMunicipio, False, False)
camposReducidosDiaMunicipio = datosReducidosAEscribir(listaDiaMunicipioCantidad, diasMunicipio, False, False)
camposReducidosBarrioMunicipio = datosReducidosAEscribir(listaBarrioMunicipioCantidad, barriosMunicipio, True, False) #Hay barrios que empiezan con numero
camposReducidosZonaMunicipio = datosReducidosAEscribir(listaZonaMunicipioCantidad, zonasMunicipio, False, False)
camposReducidosEdadMunicipio = datosReducidosAEscribir(listaEdadMunicipioCantidad, edadesMunicipio, True, True) #Debo ordenar las dos listas
camposReducidosEstadoCivilMunicipio = datosReducidosAEscribir(listaEstadoCivilMunicipioCantidad, estadosCivilMunicipio, False, False)
camposReducidosEscolaridadMunicipio = datosReducidosAEscribir(listaEscolaridadMunicipioCantidad, escolaridadMunicipio, False, False)

def escribirResultadosReduccion(file, resultados):
  archivo = open(file, "a")
  archivo.write(resultados+"\n")
  archivo.close()

escribirResultadosReduccion("cantidadCasosPorCiudadMongo.csv", camposReducidosCiudad)
escribirResultadosReduccion("cantidadCasosPorGeneroDepartamentoMongo.csv", camposReducidosGenero)
escribirResultadosReduccion("cantidadCasosPorGenerosMunicipioMongo.csv", camposReducidosGeneroMunicipio)
escribirResultadosReduccion("cantidadCasosPorDiasMunicipioMongo.csv", camposReducidosDiaMunicipio)
escribirResultadosReduccion("cantidadCasosPorBarriosMunicipioMongo.csv", camposReducidosBarrioMunicipio)
escribirResultadosReduccion("cantidadCasosPorZonasMunicipioMongo.csv", camposReducidosZonaMunicipio)
escribirResultadosReduccion("cantidadCasosPorEdadesMunicipioMongo.csv", camposReducidosEdadMunicipio)
escribirResultadosReduccion("cantidadCasosPorEstadosCivilMunicipioMongo.csv", camposReducidosEstadoCivilMunicipio)
escribirResultadosReduccion("cantidadCasosPorEscolaridadMunicipioMongo.csv", camposReducidosEscolaridadMunicipio)

#Probar que los resultados dan iguales guardandolos en un archivoSuma y los tiempos en archivoTiempos
archivoTiempos = open("tiemposMRPython.txt", "a")
if posicionEnElVector == maximaPosVector:
	archivoTiempos.write(str(tiempoTranscurrido)+"\n")
else:
	archivoTiempos.write(str(tiempoTranscurrido)+",")

archivoTiempos.close()