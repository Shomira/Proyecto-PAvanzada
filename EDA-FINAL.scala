// Databricks notebook source
// MAGIC %md
// MAGIC # Esquema creado segun las especificaciones de los datos.

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
val myDataSchem = StructType(
  Array(
    StructField("id", DecimalType(26, 0), true),
    StructField("anio", IntegerType, true),
    StructField("mes", IntegerType, true),
    StructField("provincia", IntegerType, true),
    StructField("canton", IntegerType, true),
    StructField("area", StringType, true),
    StructField("genero", StringType, true),
    StructField("edad", IntegerType, true),
    StructField("estado_civil", StringType, true),
    StructField("nivel_de_instruccion", StringType, true),
    StructField("etnia", StringType, true),
    StructField("ingreso_laboral", IntegerType, true),
    StructField("condicion_actividad", StringType, true),
    StructField("sectorizacion", StringType, true),
    StructField("grupo_ocupacion", StringType, true),
    StructField("rama_actividad", StringType, true),
    StructField("factor_expansion", DoubleType, true)
  )
);

// COMMAND ----------

// MAGIC %md
// MAGIC ## Carga de Datos (Data General)

// COMMAND ----------

val data = spark
    .read
    .schema(myDataSchem)
    .option("delimiter", "\t")
    .option("header","true")
    .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Carga de Datos(Provincias)

// COMMAND ----------

val dataProv = spark
    .read
    .option("delimiter", ";")
    .option("header","true")
    .option("inferSchema", "true")
    .csv("/FileStore/tables/cvsProvincias.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Carga de Datos(Cantones)

// COMMAND ----------

val dataCant = spark
    .read
    .option("delimiter", ",")
    .option("header","true")
    .option("inferSchema", "true")
    .csv("/FileStore/tables/Cantones.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Construcción de nuestro DataFrame Final

// COMMAND ----------

val innerProvince = data.join(dataProv, "provincia")
val dataProvCantones = innerProvince.join(dataCant, innerProvince("canton") === dataCant("codigoCanton"))
val dataProvCant = dataProvCantones.drop("provincia", "canton", "codigoCanton")

// COMMAND ----------

// MAGIC %md
// MAGIC ## FRECUENCIA DE DATOS EN LAS ETNIAS QUE PARTICIPARON EN LA ENCUESTA
// MAGIC Podemos ver que la frecuencia no es nada equitativa, y para realizar un mejor análisis solo se considerarán las cuatro etnias que más han participado en la encuesta.

// COMMAND ----------

display(dataProvCant.groupBy("etnia").count.sort(desc("count")))

// COMMAND ----------

// MAGIC %md
// MAGIC ## DataFrames de Interés

// COMMAND ----------

val dataInd = dataProvCant.where($"etnia" === "1 - Indígena")
val dataMon = dataProvCant.where($"etnia" === "5 - Montubio")
val dataMes = dataProvCant.where($"etnia" === "6 - Mestizo")
val dataBla = dataProvCant.where($"etnia" === "7 - Blanco")

val data4Etnias = dataProvCant.where($"etnia" === "1 - Indígena" || $"etnia" === "5 - Montubio" || $"etnia" === "6 - Mestizo" || $"etnia" === "7 - Blanco") 

// COMMAND ----------

// MAGIC %md
// MAGIC # Ingreso Laboral máximo de cada etnia (Global)
// MAGIC La etnia Mestizo se encuentra con el ingreso laboral más elevado de 146030 llevando una ventaja abismal en comparación a las demás; la etnia Blanco con 60000, que considerando que la población que participo en la encuesta de Indígena y Montubio es más elevada en comparación con etnia Blanco aun así el ingreso laboral es más bajo.

// COMMAND ----------

display(data4Etnias.groupBy("etnia").agg(max("ingreso_laboral")as "Ingreso Laboral Maximo").sort("etnia"))

// COMMAND ----------

// MAGIC %md
// MAGIC # Ingreso Laboral  mínimo de cada etnia (Global)
// MAGIC Aunque la tabla no muestra datos tan relevantes es muy importante tomar en cuenta que hay personas que en el campo Ingreso Laboral consta la cantidad cero. Mas adelante tomaremos en cuenta estos datos para realizar un análisis más detallado.

// COMMAND ----------

display(data4Etnias.groupBy("etnia").agg(min("ingreso_laboral")as "Ingreso Laboral Minimo").sort("etnia"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ingreso Laboral máximo de cada etnia (Por año)
// MAGIC En la etnia MESTIZO es evidente que Su ingreso laboral máximo se mantiene con una gran ventaja respecto a las otras etnias en cada año, aún considerando que en el año 2019 su ingreso laboral maximo fue de 82200 casi la mitad del ingreso laboral que en el año 2017. El año 2019, de igual forma fue el más bajo para las etnias Blanco y Montubio; pero la etnia Indígena aumentó al menos 3 veces mas que en los años anteriores

// COMMAND ----------

display(data4Etnias.groupBy("anio").pivot("etnia").max("ingreso_laboral").sort("anio"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ingreso Laboral promedio de cada etnia (Global)
// MAGIC El ingreso laboral promedio de cada etnia arrojó los resultados de que las etnia BLANCO y MESTIZO son las etnias con un ingreso laboral promedio más alto la primera con una cantidad de 634 y la segunda con 517, la etnia MONTUBIO y INDÍGENA tienen un promedio similar entre los 300 dólares.

// COMMAND ----------

display(data4Etnias.groupBy("etnia").agg(round(avg("ingreso_laboral"))as "Ingreso Laboral Promedio").sort("etnia"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ingreso Laboral promedio de cada etnia (Por año)
// MAGIC El ingreso laboral promedio de cada etnia arrojó los resultados de que las etnia BLANCO y MESTIZO son las etnias con un ingreso laboral promedio más alto la primera con una cantidad de 634 y la segunda con 517, la etnia MONTUBIO y INDÍGENA tienen un promedio similar entre los 300 dólares.

// COMMAND ----------

display(data4Etnias.groupBy("anio").pivot("etnia").agg(round(avg("ingreso_laboral"))).sort("anio"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Porcentaje donde el campo Ingreso Laboral es Nulo, de acuerdo a cada Etnia
// MAGIC En las gráficas de cada etnia muestran los resultados en porcentajes de la cantidad de personas que en el campo de ingreso laboral lo dejaron vacío, dado que es una consulta que retorna si es verdadero o falso,  true significa que el campo de ingreso laboral es NULO(esta vacío), y false si el campo de ingreso laboral tiene datos.

// COMMAND ----------

display(data4Etnias.groupBy($"ingreso_laboral".isNull as "Ingreso Laboral nulo").pivot("etnia").count)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Porcentaje donde Ingreso Laboral sea menor al salario básico, de acuerdo a cada Etnia
// MAGIC Para el análisis se ha considerado que el Ingreso laboral mínimo es $400, la gráfica muestra por cada etnia el total de personas en porcentajes que tienen un ingreso laboral menor al básico.

// COMMAND ----------

display(data4Etnias.groupBy($"ingreso_laboral" < 400 as "Ingreso laboral Menor que el Básico").pivot("etnia").count)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Sector en el que se ubican las personas que tienen un ingreso laboral de 0
// MAGIC La gráfica muestra por cada etnia un número de personas que pertenecen a dicho sector y que su ingreso laboral es nulo , las primeras barras de la izquierda que no tienen un nombre que las identifique representa a las personas que tienen como ingreso laboral 0 y en sector no pertenecen a ninguno. Aquí ciertamente llama la atención que el empleo domestico no sobresale, pues es el sector al que comunmente se le atribuye el no tener ingresos.

// COMMAND ----------

display(data4Etnias.filter($"ingreso_laboral" ===0).groupBy("sectorizacion").pivot("etnia").count.sort("sectorizacion"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Distribución de los Grupos de Ocupación según cada Etnia
// MAGIC En la gráfica podemos observar que para las etnias **Mestizo** y **Blanco** sus datos se ubican con mayor frecuencia en el grupo **Trabajad. de los servicios y comerciantes**; la etnia **Montubio** en el grupo **Trabajad. calificados agropecuarios y pesquero**; y la **Mestiza** en **Trabajadores no calificados, ocupaciones elementales**. 
// MAGIC Cabe recalcar además, que estos grupos son frecuentes en todas las etnias.

// COMMAND ----------

display(data4Etnias.groupBy($"grupo_ocupacion").pivot("etnia").count.sort("grupo_ocupacion"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Distribución de la Sectorización según cada Etnia
// MAGIC Dada la frecuencia de cada etnia que visualizamos al inicio se puede deducir que en las etnias Mestizo y Blanco, alrededor de la mitad se encuentran en el sector formal, pero en las etnias Indígenas y Montubio un porcentaje muy bajo de su población se encuentran en este sector. 

// COMMAND ----------

display(data4Etnias.groupBy("sectorizacion").pivot("etnia").count.sort("sectorizacion"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Distribución de los Niveles de Instrucción según cada Etnia
// MAGIC En el nivel de instrucción PRIMARIA es donde se ubica la mayor parte de población de cada una de las 4 etnias analizadas. De estas etnias resalta sobretodo la  **Montubio**, en la cual casi la mitad de su poblacion total está ubicado en este nivel; siendo más del doble que aquellos ubicados en el segundo nivel mas frecuente (Secundaria)

// COMMAND ----------

display(data4Etnias.groupBy("nivel_de_instruccion").pivot("etnia").count.sort("nivel_de_instruccion"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## ¿Que porcentaje de personas que tienen un nivel de instrucción "Superior Universitario" ganan menos que el salario básico?
// MAGIC La fracción azul representa los datos nulos, es decir que no ingresaron un valor.
// MAGIC La fracción naranja representa las personas que tienen un nivel de instrucción universitario y que su ingreso laboral es menor que el salario básico.
// MAGIC La fracción verde representa el porcentaje de la población que cuenta con un nivel de instrucción universitario y que su ingreso laboral es mayor que el salario básico. En cada una de las etnias vemos que el porcentaje es mayor al 50% con esto podemos afirmar que las personas que cuentan con un nivel de instrucción universitario tienen una mejor posibilidad de tener un sueldo mayor al salario básico.

// COMMAND ----------

// DBTITLE 0, 
display(data4Etnias.filter($"nivel_de_instruccion" === "09 - Superior Universitario" ).groupBy($"ingreso_laboral" < 400 as "Ingreso Laboral Menor que el Básico").pivot("etnia").count)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Distribución de las Ramas de Actividad según cada Etnia
// MAGIC Para cada una de las etnias, se puede visualizar que la rama  **A. Agricultura, ganadería caza y silvicultura y pesca** es la más frecuente,y con gran ventaja, en los datos; seguida, así mismo en todas las etnias, por la rama  **G. Comercio, reparación vehículos**

// COMMAND ----------

display(data4Etnias.groupBy($"rama_actividad").pivot($"etnia").count.sort("rama_actividad"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Distribución de personas en cada rama de actividad de aquellos ubicados en el SECTOR INFORMAL según cada etnia
// MAGIC En esta gráfica, basados en la distrubución de las ramas de actividad, nos damos cuenta que la mayor parte de los que se encontraban en la rama de actividad **A. Agricultura, ganadería caza y silvicultura y pesca** se ubican en el sector informal; siendo mas notorio en la etnia **Indídena**, pues casi su totalidad se ha encontrado en este sector

// COMMAND ----------

display(data4Etnias.filter($"sectorizacion" === "2 - Sector Informal").groupBy($"rama_actividad").pivot($"etnia").count.sort("rama_actividad"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Distribución de personas en cada rama de actividad que tengan un nivel de instrucción primaria según cada etnia
// MAGIC Aquí vemos que gran parte de la los que se encuetran en la rama **A. Agricultura, ganadería caza y silvicultura y pesca**, han correspondido a aquellos que se encuantan en el nivel de instruccion **Primaria**, esta ves constando aproximadamente la mitad de los que se encontraban en esta rama.

// COMMAND ----------

display(data4Etnias.filter($"nivel_de_instruccion" === "04 - Primaria").groupBy($"rama_actividad").pivot($"etnia").count.sort("rama_actividad"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Distribución de personas en cada rama de actividad que tengan un nivel de instrucción secundaria según cada etnia
// MAGIC En esta gráfica, ya podemos notar un cambio en la etnia **Mestizo** pues la rama **G. Comercio, reparación vehículos** se posiciono como la de mayor freccuencia, mostrandonos que en esta etnia, un solo nivel mayor de instrucción permite el paso de productores, a trabajos más especializados.

// COMMAND ----------

display(data4Etnias.filter($"nivel_de_instruccion" === "06 - Secundaria").groupBy($"rama_actividad").pivot($"etnia").count.sort("rama_actividad"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Numero de mujeres por cada año y por etnia que pertenecieron a la rama_actividad  Agricultura, ganadería caza y silvicultura y pesca
// MAGIC Podemos observar en la gráfica, que en esta rama para las etnias **Mestizo** y **Blanco** en cierta forma se ha mantenido la frecuencia de mujeres, sin variaciones muy significativas. En cambio, para la etnia **Montubio** ha tenido un incremento a través de los años, contrario a la **Indígena** que va en decremento. 

// COMMAND ----------

display(data4Etnias.filter($"rama_actividad" === "01 - A. Agricultura, ganadería caza y silvicultura y pesca" && $"genero" === "2 - Mujer").groupBy($"anio").pivot($"etnia").count.sort("anio"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### OUTLIERS INGRESO LABORAL

// COMMAND ----------

println("Data Indigenas")
dataInd.select("ingreso_laboral").summary().show()

val dfSueldoInd = dataInd.select("ingreso_laboral").where($"ingreso_laboral".isNotNull)
val avgS = dfSueldoInd.select(mean("ingreso_laboral")).first()(0).asInstanceOf[Double]
val stdDesvS = dfSueldoInd.select(stddev("ingreso_laboral")).first()(0).asInstanceOf[Double]
val inferiorS = avgS - 3 * stdDesvS
val superiorS = avgS + 3 * stdDesvS
println("Data Montubios")

dataMon.select("ingreso_laboral").summary().show()

val dfSueldoMon = dataMon.select("ingreso_laboral").where($"ingreso_laboral".isNotNull)
val avgSMon = dfSueldoMon.select(mean("ingreso_laboral")).first()(0).asInstanceOf[Double]
val stdDesvSMon = dfSueldoMon.select(stddev("ingreso_laboral")).first()(0).asInstanceOf[Double]
val inferiorSMon = avgSMon - 3 * stdDesvSMon
val superiorSMon = avgSMon + 3 * stdDesvSMon
println("Data Mestizos")
dataMes.select("ingreso_laboral").summary().show()

val dfSueldoMes = dataMes.select("ingreso_laboral").where($"ingreso_laboral".isNotNull)
val avgSMes = dfSueldoMes.select(mean("ingreso_laboral")).first()(0).asInstanceOf[Double]
val stdDesvSMes = dfSueldoMes.select(stddev("ingreso_laboral")).first()(0).asInstanceOf[Double]
val inferiorSMes = avgSMes - 3 * stdDesvSMes
val superiorSMes = avgSMes + 3 * stdDesvSMes
dataBla.select("ingreso_laboral").summary().show()

val dfSueldoBla = dataBla.select("ingreso_laboral").where($"ingreso_laboral".isNotNull)
val avgSBla = dfSueldoBla.select(mean("ingreso_laboral")).first()(0).asInstanceOf[Double]
val stdDesvSBla = dfSueldoBla.select(stddev("ingreso_laboral")).first()(0).asInstanceOf[Double]
val inferiorSBla = avgSBla - 3 * stdDesvSBla
val superiorSBla = avgSBla + 3 * stdDesvSBla

val dataOutliersIngreso = data4Etnias.where(($"etnia" === "1 - Indígena" && $"ingreso_laboral" > superiorS) || ($"etnia" === "5 - Montubio" && $"ingreso_laboral" > superiorSMon) || ($"etnia" === "6 - Mestizo" && $"ingreso_laboral" > superiorSMes) || ($"etnia" === "7 - Blanco" && $"ingreso_laboral" > superiorSBla))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ingresos Laborales mínimos de los outliers de cada etnia 
// MAGIC La gráfica muestra el ingreso laboral mínimo de la data con outliers, Se puede observar que el ingreso laboral mas bajo es de 1525 en la etnia Indígena y la etnia que tiene el ingreso laboral mínimo más alto es la etnia Blanco con 4900.

// COMMAND ----------

display(dataOutliersIngreso.groupBy("etnia").agg(min("ingreso_laboral")as "Ingreso Laboral Minimo").sort("etnia"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Distribución según el grupo ocupación de cada etnia (Outliers Ingreso laboral)
// MAGIC A diferencia de la data general, aquí podemos observar que para la etnia **Mestizo** y **Blanco** prima el grupo de **Profesionales científicos e intelectuales**. En la etnia **Indígena** igualmente se ve un cambio, siendo ahora el principal grupo el de **Trabajad. de los servicios y comerciantes**. En la etnia **Montubio** pasa algo aun mas curioso, pues su grupo (**Trabajad. calificados agropecuarios y pesqueros**) se mantuvo igual con los datos que son Outliers

// COMMAND ----------

display(dataOutliersIngreso.groupBy($"grupo_ocupacion").pivot("etnia").count.sort("grupo_ocupacion"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Distribución según el nivel de instrucción de cada etnia (Outliers Ingreso laboral)
// MAGIC Aquí tambien podemos observar cambios respecto a la data general, pues para todas las etnias ahora prima el nivel **Superior Universitario**, incluso, para la etnia **Mestizo** es coonsiderable el nivel **Post-grado**, pero en las etnias **Indígena** y **Montubio** el nivel **Primaria** y **Secunadaria** tiene un peso casi igual que el  **Superior Universitario**, lo que indicaría que en estas etnias el exito economico no esta determinado por el nivel de instrucción como sucede en las otras etnias.

// COMMAND ----------

display(dataOutliersIngreso.groupBy($"nivel_de_instruccion").pivot("etnia").count.sort("nivel_de_instruccion"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Distribución según la provincia de cada etnia (Outliers Ingreso laboral)
// MAGIC Se procedió hacer el análisis de las personas que tienen los sueldos más elevados, ¿En qué provincia del país se encuentran?. Se encuentran en la provincia de PICHINCHA que es es la que resalta en relación con las otras provincias, seguido esta TUNGURAHUA, GUAYAS Y AZUAY .
// MAGIC La gráfica destaca la mayor densidad de personas con relación a indígenas y blancos, en las provincias de Guayas, Los Ríos, Manabí y Pichincha.

// COMMAND ----------

display(dataOutliersIngreso.groupBy($"nomProvincia").pivot("etnia").count.sort("nomProvincia"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### OUTLIERS EDAD

// COMMAND ----------

println("Etnia Indigena")
dataInd.select("edad").summary().show()
val dfEdadInd = dataInd.select("edad").where($"edad".isNotNull)
val avgE = dfEdadInd.select(mean("edad")).first()(0).asInstanceOf[Double]
val stdDesvE = dfEdadInd.select(stddev("edad")).first()(0).asInstanceOf[Double]
val inferiorE = avgE - 3 * stdDesvE
val superiorE = avgE + 3 * stdDesvE
println("Etnia Montubio")
dataMon.select("edad").summary().show()
val dfEdadMon = dataMon.select("edad").where($"edad".isNotNull)
val avgEMon = dfEdadMon.select(mean("edad")).first()(0).asInstanceOf[Double]
val stdDesvEMon = dfEdadMon.select(stddev("edad")).first()(0).asInstanceOf[Double]
val inferiorEMon = avgEMon  - 3 * stdDesvEMon
val superiorEMon = avgEMon + 3 * stdDesvEMon
println("Etnia Mestizo")
dataMes.select("edad").summary().show()
val dfEdadMes = dataMes.select("edad").where($"edad".isNotNull)
val avgEMes = dfEdadMes.select(mean("edad")).first()(0).asInstanceOf[Double]
val stdDesvEMes = dfEdadMes.select(stddev("edad")).first()(0).asInstanceOf[Double]
val inferiorEMes = avgEMes - 3 * stdDesvEMes
val superiorEMes = avgEMes + 3 * stdDesvEMes
dataBla.select("edad").summary().show()
val dfEdadBla = dataBla.select("edad").where($"edad".isNotNull)
val avgEBla = dfEdadBla.select(mean("edad")).first()(0).asInstanceOf[Double]
val stdDesvEBla = dfEdadBla.select(stddev("edad")).first()(0).asInstanceOf[Double]
val inferiorEBla = avgEBla - 3 * stdDesvEBla
val superiorEBla = avgEBla + 3 * stdDesvEBla

val dataOutliersEdad = data4Etnias.where(($"etnia" === "1 - Indígena" && $"edad" > superiorE) || ($"etnia" === "5 - Montubio" && $"edad" > superiorEMon) || ($"etnia" === "6 - Mestizo" && $"edad" > superiorEMes) || ($"etnia" === "7 - Blanco" && $"edad" > superiorEBla))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Edad mínima de los outliers de cada etnia 
// MAGIC Por cada etnia se ha realizado el análisis de la edad mínima de las etnias, observando que la edad mínima menor es 87 en la etnia **Mestizo** y la edad mínima más alta es 96 en la etnia **Montubio**. 

// COMMAND ----------

display(dataOutliersEdad.groupBy("etnia").agg(min("edad")as "Edad Mínima").sort("etnia"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ingresos Laborales promedio de los outliers de edad de cada etnia
// MAGIC El menor promedio de ingreso lo tenemos en la etnia Indígena, que es de 45 $, y el ingreso promedio más alto está en la etnia Montubio con 381 $.
// MAGIC Se sabe que las personas consideradas aquí tienen mínimo 87 años, y  han llegado a ganar un sueldo mucho menor que el salario básico, llegando a no ser ni la cuarta parte del mismo.

// COMMAND ----------

// DBTITLE 0, 
display(dataOutliersEdad.groupBy("etnia").agg(avg("ingreso_laboral")as "Ingreso Laboral Promedio").sort("etnia"))

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Distribución según el nivel de instrucción de cada etnia (Outliers Edad)
// MAGIC Analizando el nivel de instrucción de esta población, por cada etnia se evidencia que la **Mestizo** e **Indígena** radican en los niveles **Primaria** y **Ninguno**. Implicando esto la importancia de un buen nivel de instrucción.

// COMMAND ----------

display(dataOutliersEdad.groupBy($"nivel_de_instruccion").pivot("etnia").count.sort("nivel_de_instruccion"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## SQL
// MAGIC Creación de nuestra tabla dinámica

// COMMAND ----------

dataProvCant.createOrReplaceTempView("EDU_TABLE")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Nombres de cada provincias junto a: el número de etnias que están presentes en esa provincia, el promedio del ingreso laboral anual de todos los registrados en dicha provincia y el número de registros que existen en ésta

// COMMAND ----------

// MAGIC %sql
// MAGIC Select nomProvincia, count(DISTINCT etnia) , avg(ingreso_laboral * 12) , count(*)
// MAGIC From EDU_TABLE
// MAGIC GROUP BY nomProvincia
// MAGIC ORDER BY 4;

// COMMAND ----------

// MAGIC %md
// MAGIC ### Nombre y cantidad de registros de cada provincia que tengan un ingreso laboral mayor a 20000 y se encuentren en el grupo de Profesionales científicos e intelectuales

// COMMAND ----------

// MAGIC %sql
// MAGIC Select nomProvincia, count(*)
// MAGIC From EDU_TABLE
// MAGIC WHERE ingreso_laboral > 20000 AND grupo_ocupacion == "02 - Profesionales científicos e intelectuales"
// MAGIC GROUP BY nomProvincia
// MAGIC ORDER BY count(*);

// COMMAND ----------

// MAGIC %md
// MAGIC ### Grupo de ocupación y cantidad de registros de aquellos que se encuentran en las provincias de Pichincha o Guayas, con un nivel de instrucción de Post-grado y con un ingreso laboral mayor a 10000

// COMMAND ----------

// MAGIC %sql
// MAGIC Select grupo_ocupacion, count(*)
// MAGIC From EDU_TABLE
// MAGIC WHERE (nomProvincia == "PICHINCHA" OR nomProvincia == "GUAYAS") AND nivel_de_instruccion == "10 - Post-grado" AND ingreso_laboral > 10000
// MAGIC GROUP BY grupo_ocupacion
// MAGIC ORDER BY count(*); 
