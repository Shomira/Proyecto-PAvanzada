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
// MAGIC ## Carga de Datos

// COMMAND ----------

val data = spark
    .read
    .schema(myDataSchem)
    .option("delimiter", "\t")
    .option("header","true")
    .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC ## FRECUENCIA DE DATOS EN LAS 4 ETNIAS PRINCIPALES

// COMMAND ----------

display(data.groupBy("etnia").count.sort(desc("count")))

// COMMAND ----------

val dataInd = data.where($"etnia" === "1 - Indígena")
val dataMon = data.where($"etnia" === "5 - Montubio")
val dataMes = data.where($"etnia" === "6 - Mestizo")
val dataBla = data.where($"etnia" === "7 - Blanco")

val data4Etnias = data.where($"etnia" === "1 - Indígena" || $"etnia" === "5 - Montubio" || $"etnia" === "6 - Mestizo" || $"etnia" === "7 - Blanco") 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Salarios máximos de cada etnia (Global)
// MAGIC La etnia Mestizo se encuentra con el ingreso laboral más elevado de 146030 llevando una ventaja abismal en comparación a las demás; la etnia Blanco con 60000, que considerando que la población que participo en la encuesta de Indígena y Montubio es más elevada en comparación con etnia Blanco aun así el ingreso laboral es más bajo.

// COMMAND ----------

// DBTITLE 0,Salarios máximos de cada etnia (Global)
display(data4Etnias.groupBy("etnia").agg(max("ingreso_laboral")as "Ingreso Laboral Maximo").sort("etnia"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Salarios mínimos de cada etnia (Global)
// MAGIC Aunque la tabla no muestra datos tan relevantes es muy importante tomar en cuenta que hay personas que en el campo Ingreso Laboral consta la cantidad cero. Mas adelante tomaremos en cuenta estos datos para realizar un análisis más detallado.

// COMMAND ----------

// DBTITLE 0,Salarios mínimos de cada etnia (Global)
display(data4Etnias.groupBy("etnia").agg(min("ingreso_laboral")as "Ingreso Laboral Minimo").sort("etnia"))

// COMMAND ----------

// DBTITLE 1,Salarios máximos de cada etnia (Por año)
display(data4Etnias.groupBy("anio").pivot("etnia").max("ingreso_laboral").sort("anio"))

// COMMAND ----------

// DBTITLE 1,Salarios mínimos de cada etnia (Por años)
display(data4Etnias.groupBy("anio").pivot("etnia").min("ingreso_laboral").sort("anio"))

// COMMAND ----------

// DBTITLE 1,Salario promedio de cada etnia (Global)
display(data4Etnias.groupBy("etnia").agg(round(avg("ingreso_laboral"))as "Ingreso Laboral Promedio").sort("etnia"))

// COMMAND ----------

// DBTITLE 1,Salario promedio de cada etnia (Por año)
display(data4Etnias.groupBy("anio").pivot("etnia").agg(round(avg("ingreso_laboral"))).sort("anio"))

// COMMAND ----------

// DBTITLE 1,Porcentaje donde Ingreso Laboral sea menor al salario básico, de acuerdo a cada Etnia
display(data4Etnias.groupBy($"ingreso_laboral" < 400 as "Ingreso laboral Menor que el Básico").pivot("etnia").count)

// COMMAND ----------

// DBTITLE 1,Porcentaje donde el campo Ingreso Laboral es Nulo, de acuerdo a cada Etnia
display(data4Etnias.groupBy($"ingreso_laboral".isNull as "Ingreso Laboral nulo").pivot("etnia").count)

// COMMAND ----------

// DBTITLE 1,Distribución de los Grupos de Ocupación según cada Etnia
display(data4Etnias.groupBy($"grupo_ocupacion").pivot("etnia").count.sort("grupo_ocupacion"))

// COMMAND ----------

// DBTITLE 1,Distribución de la Sectorización según cada Etnia
display(data4Etnias.groupBy("sectorizacion").pivot("etnia").count.orderBy("sectorizacion"))

// COMMAND ----------

// DBTITLE 1,Distribución de los Niveles de Instrucción según cada Etnia
display(data4Etnias.groupBy("nivel_de_instruccion").pivot("etnia").count.orderBy("nivel_de_instruccion"))

// COMMAND ----------

// DBTITLE 1,¿Que porcentaje de personas que tienen un nivel de instrucción "Superior Universitario" ganan menos que el salario básico?
display(data4Etnias.filter($"nivel_de_instruccion" === "09 - Superior Universitario" ).groupBy($"ingreso_laboral" < 400 as "Ingreso Laboral Menor que el Básico").pivot("etnia").count)

// COMMAND ----------

// DBTITLE 1,Distribución de las Ramas de Actividad según cada Etnia
display(data4Etnias.groupBy($"rama_actividad").pivot($"etnia").count.sort("rama_actividad"))

// COMMAND ----------

// DBTITLE 1,Distribución de personas en cada rama de actividad de aquellos ubicados en el SECTOR INFORMAL según cada etnia
display(data4Etnias.filter($"sectorizacion" === "2 - Sector Informal").groupBy($"rama_actividad").pivot($"etnia").count.sort("rama_actividad"))

// COMMAND ----------

// DBTITLE 1,Distribución de personas en cada rama de actividad que tengan un nivel de instrucción primaria según cada etnia
display(data4Etnias.filter($"nivel_de_instruccion" === "04 - Primaria").groupBy($"rama_actividad").pivot($"etnia").count.orderBy("rama_actividad"))

// COMMAND ----------

// DBTITLE 1,Distribución de personas en cada rama de actividad que tengan un nivel de instrucción secundaria según cada etnia
display(data4Etnias.filter($"nivel_de_instruccion" === "06 - Secundaria").groupBy($"rama_actividad").pivot($"etnia").count.orderBy("rama_actividad"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### OUTLIERS SUELDO

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

// COMMAND ----------

val datadataOutliersIngreso = data4Etnias.where(($"etnia" === "1 - Indígena" && $"ingreso_laboral" > superiorS) || ($"etnia" === "5 - Montubio" && $"ingreso_laboral" > superiorSMon) || ($"etnia" === "6 - Mestizo" && $"ingreso_laboral" > superiorSMes) || ($"etnia" === "7 - Blanco" && $"ingreso_laboral" > superiorSBla))

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

// COMMAND ----------

val datadataOutliersEdad = data4Etnias.where(($"etnia" === "1 - Indígena" && $"edad" > superiorE) || ($"etnia" === "5 - Montubio" && $"edad" > superiorEMon) || ($"etnia" === "6 - Mestizo" && $"edad" > superiorEMes) || ($"etnia" === "7 - Blanco" && $"edad" > superiorEBla))
