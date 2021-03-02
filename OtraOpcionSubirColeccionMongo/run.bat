@echo off
title Prueba.
python particionBD.py >> tiemposMRPythonTemp.txt REM el archivo temporal es para no danar la forma en como lo he implementado en python

type tiemposMRPythonTemp.txt | python subirParticionesAMongo.py >> tiemposMRPython.txt REM el archivo txt contiene el nombre en cada linea de los archivos creados
erase tiemposMRPythonTemp.txt REM ya no me sirve

setlocal enabledelayedexpansion

set count=0
for /f "tokens=* usebackq" %%f in (tiemposMRPython.txt) do ( REM tomo cada linea del archivo txt y la meto en un vector 
	set filesNames[!count!]=%%f
	set /a count=!count!+1
)

set x=0
:LongitudArray REM calculo la longitud del vector
if defined filesNames[%x%] ( REM verifico que en una posicion este definido un elemento 
	call echo %%filesNames[%x%]%%
	set /a x+=1
	GOTO :LongitudArray REM llamada iterativa a la funcion
)
echo "la longitud del array es " %x%
set /a x-=1 
for /l %%n in (0,1,%x%) do ( REM itero de uno en uno hasta recorrer todo el vector y muestro su contenido
    echo !filesNames[%%n]!
    echo "hola"
)

pause
for /l %%i in (1,1,10) do ( REM hago 10 iteraciones
    for /l %%n in (0,1,%x%) do ( REM itero de uno en uno hasta pasando cada nombre de archivo al programa en python
        python procesamientoYTiempos.py !filesNames[%%n]!
)
)
endlocal
echo "se eliminarán los archivos creados :)"
pause
erase Registros10.csv Registros5.csv Registros3.csv Registros2.csv tiemposMRPython.txt