"""
En este script, se particionara el archivo que contiene la totalidad de registros, se usa una funcion
logaritmica simplemente para calcular el numero de registros o particiones maxima que se deberia producir.
La idea es ir partiendo un csv o cualquier otro archivo a la mitad, como esto puede dar en decimal, se toma
solo un valor entero, entonces, para evitar que algunos registros no sean tomados en cuenta, se hace una 
funcion techo (ceil), como es una funcion techo, es podible entonces, que no se creen todos los archivos indicados
por la funcion logaritmica, ya que no dan divisiones exactas.
"""

import math
import sys

file = sys.argv[1]
ext = sys.argv[2]
cantidadRegistros = sys.argv[3]

def calcularNumeroRegistroPorArchivo():
	bloques = []
	numeroRegistros = int(cantidadRegistros)
	cantidadRegistrosFaltantes = numeroRegistros
	cantidadRegistrosAsignados = 0
	cantidadBloques = int(math.floor(math.log(numeroRegistros,2)))+1
	#print cantidadBloques
	for n in range(1,cantidadBloques):	
		bloque = int(math.ceil(float(numeroRegistros)/2**n))
		bloques.append(bloque)
		cantidadRegistrosFaltantes -= bloque
		if cantidadRegistrosFaltantes == 0:
			#en la penultima posicion asigno la suma de las dos ultimas posiciones, ya que suelen ser valores pequenos
			#y esto me puede traer inconvenientes como en el caso del procesamientoyTiemposMongo, por ejemplo, si se genera
			#un archivo con solo un registro, es posible que ese registro no cumpla con alguna condicion de consulta
			#y por lo tanto no escribire resultados en el archivo correspondiente, esto ocasiona que no se ponga un salto
			#de linea en el archivo de resultados, en caso tal que este sea el ultimo archivo o coleccion a procesar y
			#tenga my pocos registros. Por ultimo, verifico que el valor de la suma de los penutimos bloques no este
			#en la lista bloques para que no ser repitan colecciones ni archivos.
			sumaUltimosBloques = bloques[-2] + bloques[-1]
			if sumaUltimosBloques == bloques[-3]:
				pass
			else:
				bloques[-2] = sumaUltimosBloques
				bloques.pop(-1)
			break
		if cantidadRegistrosFaltantes < 0:
			bloques[-1] = numeroRegistros - cantidadRegistrosAsignados
			sumaUltimosBloques = bloques[-2] + bloques[-1]
			if sumaUltimosBloques == bloques[-3]:
				pass
			else:
				bloques[-2] = sumaUltimosBloques
				bloques.pop(-1)
			break
		cantidadRegistrosAsignados += bloque 
	return bloques #por cada elemento, se crea un archivo con esa cantidad de registros. 

def bloquesDeRegistros():
	cont = 1
	bloques = calcularNumeroRegistroPorArchivo() 
	registro1 = ""
	#print bloques
	with open(file+"."+ext, "r") as datos:
		registro1 = datos.readline()
		for tamBloque in bloques:
			with open("Registros"+str(tamBloque)+".csv", "w") as bloque:
				bloque.write(registro1)
				for _ in range(tamBloque):
					linea = datos.readline()
					bloque.write(linea)
					#print linea
				bloque.close()
			print "Registros"+str(tamBloque)

if __name__ == "__main__":
	bloquesDeRegistros()