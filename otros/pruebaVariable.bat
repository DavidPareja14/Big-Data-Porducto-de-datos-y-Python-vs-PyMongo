@echo off
title TOMANDO TIEMPOS DELL PROCESAMIENTO CON PYTHON Y PYMONGO.
REM cargo el nombre de la base de datos en donde pondre todas las colecciones
set baseDatos=pruebaFinal
REM recibo el nombre del archivo el cual sera el mismo para la coleccion, es decir, el archivo que se particionara.
echo Archivo: 
set file=Delito_Violencia_Intrafamiliar
echo Ext: 
set ext=csv
REM estos campos seran los que yo crea relevantes en el proceso de seleccion de campos (DEPARTAMENTO,MUNICIPIO,DIA,BARRIO,ZONA,EDAD,SEXO,ESTADO CIVIL,ESCOLARIDAD)
echo Digite los campos con los que trabará (Todos): 
set  campos=DEPARTAMENTO,MUNICIPIO,DIA,BARRIO,ZONA,EDAD,SEXO,ESTADOCIVIL,ESCOLARIDAD
REM estos campos son los que realmente necesito para hacer el procesamiento mapRed, en este caso seran DEPARTAMENTO Y 
REM MUNICIPIO, pero para python necesito los indices, segun el orden de la variable campos
echo Con que campos desea trabajar (numero de indice, desde 0) Python (Algunos): 
REM para mi procesamiento solo necesito como tal el nombre DEPARTAMENTO y MUNICIPIO, sus indices son 0,1
set /p campoATrabajarPython=
echo Con que campos desea trabajar (nombre) Mongo (Algunos): 
REM necesito los campos DEPARTAMENTO,MUNICIPIO
set campoATrabajarMongo=DEPARTAMENTO,MUNICIPIO
REM numero de veces que cada particion se va a procesar
echo Con cuantas iteraciones quiere empezar :
set /p numIteraciones=
REM me permite poner los signos de admiracion en lugar de porcentajes.
setlocal enabledelayedexpansion 

set dirFile=C:/Users/USUARIO/Desktop/U/maestria/bigData/TrabajoFinal/!file!.!Ext!
REM si existe la coleccion file en mongo se elimina, esto es por si este script se ejecuta varias veces.
REM mongo.exe !baseDatos! --eval "db.!file!.drop();"
mongo.exe !baseDatos! --eval "db.dropDatabase();"
mongoimport --db !baseDatos! --collection !file! --type=!ext! --file !dirFile! --headerline

REM Primero debe estar la coleccion y la base de datos en mongo, para poder listar los dept y municipios
python selectores.py !baseDatos! !file!

REM con solo un mayor se sobreescribe el archivo, --quiet es para obtener el valor de retorno de la cantidad de registros "creo".
mongo.exe !baseDatos! --quiet --eval "db.!file!.find().count();" > cantidadRegistros.txt 
set /p cantidadRegistros=<cantidadRegistros.txt
echo !cantidadRegistros!

python particionBD.py %file% %ext% %cantidadRegistros% >> tiemposMRPython.txt REM el archivo temporal es para no danar la forma en como lo he implementado en python
erase cantidadRegistros.txt

set count=0
for /f "tokens=* usebackq" %%f in (tiemposMRPython.txt) do ( REM tomo cada linea del archivo txt y la meto en un vector, cada linea es el nombre de una particion. 
	set filesNames[!count!]=%%f
	set /a count=!count!+1
)

set x=0
:LongitudArray REM calculo la longitud del vector
if defined filesNames[%x%] ( REM verifico que en una posicion este definido un elemento 
	REM Se imprimen las colecciones a subir a Mongo
	call echo %%filesNames[%x%]%%
	set /a x+=1
	GOTO :LongitudArray REM llamada iterativa a la funcion
)
echo "la longitud del array con los nombres de las particiones es " %x%
REM resto para poder iterar desde 0
set /a x-=1  
REM **********************************************************************************************************
echo Iniciando procesamiento con Python...
for /l %%i in (1,1,!numIteraciones!) do ( 
    for /l %%n in (0,1,%x%) do ( REM itero de uno en uno PARA leer los archivos y pasar su contenido
    echo .
	set filePart=!filesNames[%%n]!.!ext!
	python cargarCamposPython.py !filePart! !campos! | python mapPython.py !campoATrabajarPython! | python reduceyTiemposPython.py %%n !x!
)
)
echo Procesamiento finalizado con Python.
REM **********************************************************************************************************
echo/
echo Subiendo archivos particionados (colecciones) a Mongo
echo/
for /l %%n in (0,1,%x%) do ( REM itero de uno en uno hasta recorrer todo el vector
    set coleccion=!filesNames[%%n]!
    set files=C:/Users/USUARIO/Desktop/U/maestria/bigData/TrabajoFinal/!filesNames[%%n]!.csv
    REM se puede poner tambien %file% gracias a setlocal enabledelayedexpansion se puede poner !!
    echo subiendo coleccion !filesNames[%%n]!...
    REM se pasan argumentos al shell de mongo para eliminar las colecciones en caso de que este script se ejecute mas de una vez (ya no, se elim la BD).

    REM mongo.exe !baseDatos! --eval "db.!coleccion!.drop();"
    mongoimport --db !baseDatos! --collection !coleccion! --type=!ext! --file !files! --headerline 
)
echo/
echo ******************************************************
echo SE HAN SUBIDO TODAS LAS PARTICIONES (COLECCIONES) A MONGO
echo ******************************************************

echo/
echo "se eliminarán los archivos particionados creados :)"
pause
cls
for /l %%n in (0,1,%x%) do (
    erase !filesNames[%%n]!.csv
)

echo Se estan seleccionando los campos (columnas) para Mongo...

for /l %%n in (0,1,%x%) do ( REM itero de uno en uno pasando cada nombre de archivo al programa en python
    python cargarCamposMongo.py !baseDatos! !filesNames[%%n]! !campos!
)

echo Iniciando procesamiento con Mongo...
for /l %%i in (1,1,!numIteraciones!) do ( 
    for /l %%n in (0,1,%x%) do ( REM itero de uno en uno pasando cada nombre de archivo al programa en python
    	echo .
        python procesamientoYTiemposMongo.py !filesNames[%%n]! %%n !x! !baseDatos! !campoATrabajarMongo!
)
)
echo Procesamiento finalizado con Mongo.

python comprobacionResultadosProcesamiento.py !baseDatos! !file! !numIteraciones! !campoATrabajarMongo! !x!
echo/
echo A continuacion se mostrara el producto de datos
pause
python graficasMapeoReduccion.py
echo/
echo Se mostraran los tiempos
pause
python graficasTiempos.py !x! !numIteraciones!

for /l %%n in (0,1,%x%) do (
    echo !filesNames[%%n]!
)

pause
erase tempCamposATrabajar.txt tiemposMRPython.txt temporalDeComprobacionParaGraficasMapRed.txt

del cantidadCasosPorCiudadPython.csv
del cantidadCasosPorGeneroDepartamentoPython.csv
del cantidadCasosPorGenerosMunicipioPython.csv
del cantidadCasosPorDiasMunicipioPython.csv
del cantidadCasosPorBarriosMunicipioPython.csv
del cantidadCasosPorZonasMunicipioPython.csv
del cantidadCasosPorEdadesMunicipioPython.csv
del cantidadCasosPorEstadosCivilMunicipioPython.csv
del cantidadCasosPorEscolaridadMunicipioPython.csv

del cantidadCasosPorCiudadMongo.csv
del cantidadCasosPorGeneroDepartamentoMongo.csv
del cantidadCasosPorGenerosMunicipioMongo.csv
del cantidadCasosPorDiasMunicipioMongo.csv
del cantidadCasosPorBarriosMunicipioMongo.csv
del cantidadCasosPorZonasMunicipioMongo.csv
del cantidadCasosPorEdadesMunicipioMongo.csv
del cantidadCasosPorEstadosCivilMunicipioMongo.csv
del cantidadCasosPorEscolaridadMunicipioMongo.csv