@echo off
title Prueba.
REM cargo el nombre de la base de datos en donde pondre todas las colecciones
set baseDatos=pruebaFinal
REM recibo el nombre del archivo el cual sera el mismo para la coleccion.
echo archivo: 
set /p file= 
echo Ext: 
set /p ext=

REM me permite poner los signos de admiracion en lugar de porcentajes.
setlocal enabledelayedexpansion 

set dirFile=C:/Users/USUARIO/Desktop/U/maestria/bigData/TrabajoFinal/!file!.!Ext!
REM si existe la coleccion file en mongo se elimina, esto es por si este script se ejecuta varias veces.
mongo.exe !baseDatos! --eval "db.!file!.drop();"
mongoimport --db !baseDatos! --collection !file! --type=!ext! --file !dirFile! --headerline 
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
set numIteraciones=10
REM **********************************************************************************************************
echo Iniciando procesamiento con Python...
for /l %%i in (1,1,!numIteraciones!) do ( 
    for /l %%n in (0,1,%x%) do ( REM itero de uno en uno PARA leer los archivos y pasar su contenido
	set filePart=!filesNames[%%n]!.csv 
	type !filePart! | python mapPython.py | python reduceyTiemposPython.py %%n !x!
)
)

echo Procesamiento finalizado con Python.
REM **********************************************************************************************************

for /l %%n in (0,1,%x%) do ( REM itero de uno en uno hasta recorrer todo el vector
    set coleccion=!filesNames[%%n]!
    set files=C:/Users/USUARIO/Desktop/U/maestria/bigData/TrabajoFinal/!filesNames[%%n]!.csv
    REM se puede poner tambien %file% gracias a setlocal enabledelayedexpansion se puede poner !!
    echo subiendo coleccion !filesNames[%%n]!...
    REM se pasan argumentos al shell de mongo para eliminar las colecciones en caso de que este script se ejecute mas de una vez.

    mongo.exe !baseDatos! --eval "db.!coleccion!.drop();"
    mongoimport --db !baseDatos! --collection !coleccion! --type=!ext! --file !files! --headerline 
)
echo/
echo ******************************************************
echo SE HAN SUBIDO TODAS LAS PARTICIONES (COLECCIONES) A MONGO
echo ******************************************************
echo/
echo "se eliminarán los archivos particionados creados :)"
pause
for /l %%n in (0,1,%x%) do (
    erase !filesNames[%%n]!.csv
)
echo Iniciando procesamiento con Mongo...
for /l %%i in (1,1,!numIteraciones!) do ( 
    for /l %%n in (0,1,%x%) do ( REM itero de uno en uno pasando cada nombre de archivo al programa en python
        python procesamientoYTiemposMongo.py !filesNames[%%n]! %%n !x! !baseDatos!
)
)
echo Procesamiento finalizado con Mongo.

python comprobacionResultadosProcesamiento.py !baseDatos! !file! !numIteraciones!

python graficas.py !x! !numIteraciones!

endlocal
pause
erase tiemposMRPython.txt resultadosMRPythonMongo.txt resultadosMRPython.txt