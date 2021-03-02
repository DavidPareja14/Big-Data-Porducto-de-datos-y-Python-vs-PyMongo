# Big Data: Porducto de datos y Python vs PyMongo
Para este proyecto se hace uso de la base de datos Violencia intrafamiliar en Colombia durante el año 2015. Para generar información relevante se utiliza el paradigma Map-Reduce, luego se muestran gráficos importantes sobre los departamentos, ciudades, barrios, personas, zonas (entre otros, los cuales suman 8 gráficos en total) más afectadas y por último se hace una comparativa de rendimiento entre hacer Map-Reduce solo con Python o hacerlo con PyMongo.

Para fines de este trabajo, la base de datos a utilizar no se empleará directamente, en su lugar, se va a partir en varios trozos y sobre estos trozos se realizará el mapeo y reducción a que haya lugar (al final, cada trozo será una colección en la base de datos). 

El fin de los trozos, es tomar el tiempo que el mapeo y reducción tarde en procesar cada uno para luego poder comparar en gráficas de rendimiento, qué puede resultar más eficiente (en término del tiempo), si hacer el procesamiento solo con Python (lo cual personalmente no lo recomiendo por las complejidades que implica) o realizarlo junto a pymongo, el cual ya trae el map reduce implementado.

## Instrucciones
* Para empezar con la ejecución del programa, se debe ejecutar el archivo **run.bat** (No se preocupen si el antivirus se activa, el programa solo accede a archivos que el propio programa genera, no lo he creado para perjudicar a alguien).

* El programa pedirá dos cosas:
    1. Ingresar los campos sobre los cuales se desea hacer el análisis. Aquí hay que ingresar 0,1 tal cual, estos dos valores indican que el 0 es para el campo departamento y el 1 es para ciudad.
    2. Cuantas iteraciones desea que se realice, aquí aconsejo un número pequeño, con 3 es suficiente, ya que es demorado el proceso (a no ser que cuente con una computadora con buenas carácterísticas). Las iteraciones indican cuantas veces se va a ejecutar cada trozo para realizar el mapeo y reducción. Hay que recordar que por cada iteración se toma nota del tiempo que se tarde en procesar cada trozo, 
    3. Se debe seleccionar el departamento y la ciudad que se desee (Hay que recordar que solo es para Colombias).

* Según el número de iteraciones indicadas, hay que esperar determinado tiempo, en donde pida realizar o dar enter, se oprime este botón hasta que por fín se muestren los resultados y gráficos interesantes pertinentes a violencia intrafamiliar.

* Por último se muestran tres gráficos que comparan el rendimiento que anteriormente se ha explicado.

## Consideraciones
Este proyecto ha sido realizado con python 2.7, se debe tener instalado mongoDB y funciona solo para windows.
