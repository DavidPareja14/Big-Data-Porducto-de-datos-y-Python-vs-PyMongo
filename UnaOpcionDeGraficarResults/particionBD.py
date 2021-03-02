import math
import sys

file = sys.argv[1]
ext = sys.argv[2]
cantidadRegistros = sys.argv[3]

def calcularNumeroRegistroPorArchivo():
	bloques = []
	numeroRegistros = int(cantidadRegistros)
	cantidadRegistrosAsignados = 0
	cantidadBloques = int(math.floor(math.log(numeroRegistros,2)))+1
	#print cantidadBloques
	for n in range(1,cantidadBloques):		
		if cantidadRegistrosAsignados == numeroRegistros:
			break #Si hay muy pocos registros, no podre alcanzar la cantidad de archivos deseada (10 registros 4 blocks).
		bloque = int(math.ceil(float(numeroRegistros)/2**n))
		if n == cantidadBloques-1 and (numeroRegistros - cantidadRegistrosAsignados) != bloque:
			bloques.append(numeroRegistros - cantidadRegistrosAsignados)
			cantidadRegistrosAsignados += bloque
			break #Este condicional es por si hay un caso de 70 registros y 4 bloques. (Pensar!!!)
		bloques.append(bloque)
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