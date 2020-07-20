// Databricks notebook source
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

val data = spark
    .read
    .schema(myDataSchem)
    .option("delimiter", "\t")
    .option("header","true")
    .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------

val dataInd = data.select("etnia", "grupo_ocupacion", "nivel_de_instruccion", "ingreso_laboral", "rama_actividad", "sectorizacion","genero" ,"anio" ).where($"etnia" === "1 - Indígena")
val dataMon = data.select("etnia", "grupo_ocupacion", "nivel_de_instruccion", "ingreso_laboral", "rama_actividad", "sectorizacion","genero","anio").where($"etnia" === "5 - Montubio")
val dataMes = data.select("etnia", "grupo_ocupacion", "nivel_de_instruccion", "ingreso_laboral", "rama_actividad", "sectorizacion","genero","anio").where($"etnia" === "6 - Mestizo")

val data3Etnias = data.select("etnia", "grupo_ocupacion", "nivel_de_instruccion", "ingreso_laboral", "rama_actividad", "sectorizacion","genero" ,"anio", "edad" ).where($"etnia" === "1 - Indígena" || $"etnia" === "5 - Montubio" || $"etnia" === "6 - Mestizo") 


// COMMAND ----------

// DBTITLE 1,Salarios máximos de cada etnia (Global)
display(data3Etnias.groupBy("etnia").agg(round(max("ingreso_laboral"))as "Ingreso Laboral Maximo").orderBy("etnia"))

// COMMAND ----------

// DBTITLE 1,Salarios mínimos de cada etnia (Global)
display(data3Etnias.groupBy("etnia").agg(round(min("ingreso_laboral"))as "Ingreso Laboral Minimo").orderBy("etnia"))

// COMMAND ----------

// DBTITLE 1,Salarios máximos de cada etnia (Por año)
display(data3Etnias.groupBy("anio").pivot("etnia").agg(round(max("ingreso_laboral"))as "Ingreso Laboral Maximo").orderBy("anio"))

// COMMAND ----------

// DBTITLE 1,Salarios mínimos de cada etnia (Por años)
display(data3Etnias.groupBy("anio").pivot("etnia").agg(round(min("ingreso_laboral"))as "Ingreso Laboral Minimo").orderBy("anio"))

// COMMAND ----------

// DBTITLE 1,Salario promedio de cada etnia (Global)
display(data3Etnias.groupBy("etnia").agg(round(avg("ingreso_laboral"))as "Ingreso Laboral Promedio").orderBy("etnia"))

// COMMAND ----------

// DBTITLE 1,Salario promedio de cada etnia (Por año)
display(data3Etnias.groupBy("anio").pivot("etnia").agg(round(avg("ingreso_laboral"))as "Ingreso Laboral Promedio").orderBy("anio"))

// COMMAND ----------

// DBTITLE 1,Porcentaje donde Ingreso Laboral sea menor al salario básico, de acuerdo a cada Etnia
display(data3Etnias.groupBy("etnia").pivot($"ingreso_laboral" < 400).count.orderBy("etnia"))

// COMMAND ----------

println(f"${(dataInd.where($"ingreso_laboral" < 400).count / dataInd.count.toDouble) * 100}%.2f%% Indigenas")
println(f"${(dataMon.where($"ingreso_laboral" < 400).count / dataMon.count.toDouble) * 100}%.2f%% Montubio")
println(f"${(dataMes.where($"ingreso_laboral" < 400).count / dataMes.count.toDouble) * 100}%.2f%% Mestizo")

// COMMAND ----------

// DBTITLE 1,Porcentaje donde el campo Ingreso Laboral es Nulo, de acuerdo a cada Etnia
display(data3Etnias.groupBy("etnia").pivot($"ingreso_laboral".isNull).count.orderBy("etnia"))

// COMMAND ----------

println(f"${(dataInd.where($"ingreso_laboral".isNull).count / dataInd.count.toDouble) * 100}%.2f%% Indigenas")
println(f"${(dataMon.where($"ingreso_laboral".isNull).count / dataMon.count.toDouble) * 100}%.2f%% Montubio")
println(f"${(dataMes.where($"ingreso_laboral".isNull).count / dataMes.count.toDouble) * 100}%.2f%% Mestizo")

// COMMAND ----------

// DBTITLE 1,Distribución de los Grupos de Ocupación según cada Etnia
display(data3Etnias.groupBy($"grupo_ocupacion").pivot("etnia").count.sort("grupo_ocupacion"))

// COMMAND ----------

// DBTITLE 1,Distribución de la Sectorización según cada Etnia
display(data3Etnias.groupBy("sectorizacion").pivot("etnia").count.orderBy("sectorizacion"))

// COMMAND ----------

// DBTITLE 1,Distribución de los Niveles de Instrucción según cada Etnia
display(data3Etnias.groupBy("nivel_de_instruccion").pivot("etnia").count.orderBy("nivel_de_instruccion"))

// COMMAND ----------

// DBTITLE 1,¿Que porcentaje de personas que tienen un nivel de instrucción "Superior Universitario" ganan menos que el salario básico?
display(data3Etnias.filter($"nivel_de_instruccion" === "09 - Superior Universitario" and $"ingreso_laboral".isNotNull).groupBy("etnia").pivot($"ingreso_laboral" < "400").count.orderBy("etnia"))

// COMMAND ----------

println(f"${(dataInd.filter($"nivel_de_instruccion" === "09 - Superior Universitario" ).where($"ingreso_laboral" < "400").count / dataInd.filter($"nivel_de_instruccion" === "09 - Superior Universitario").count.toDouble) * 100}%.2f%% Indigenas")
println(f"${(dataMon.filter($"nivel_de_instruccion" === "09 - Superior Universitario" ).where($"ingreso_laboral" < "400").count / dataMon.filter($"nivel_de_instruccion" === "09 - Superior Universitario" ).count.toDouble) * 100}%.2f%% Montubio")
println(f"${(dataMes.filter($"nivel_de_instruccion" === "09 - Superior Universitario" ).where($"ingreso_laboral" < "400").count / dataMes.filter($"nivel_de_instruccion" === "09 - Superior Universitario" ).count.toDouble) * 100}%.2f%% Mestizo")
// COMMAND ----------

// DBTITLE 1,Distribución de las Ramas de Actividad según cada Etnia
display(data3Etnias.groupBy($"etnia").pivot($"rama_actividad").count.orderBy("etnia"))

// COMMAND ----------

// DBTITLE 1,Distribución de personas en cada rama de actividad de aquellos ubicados en el SECTOR INFORMAL según cada etnia
display(data3Etnias.filter($"sectorizacion" === "2 - Sector Informal").groupBy($"rama_actividad").pivot($"etnia").count.orderBy("rama_actividad"))



// COMMAND ----------

// DBTITLE 1,Distribución de personas en cada rama de actividad que tengan un nivel de instrucción primaria según cada etnia
display(data3Etnias.filter($"nivel_de_instruccion" === "04 - Primaria").groupBy($"rama_actividad").pivot($"etnia").count.orderBy("rama_actividad"))



// COMMAND ----------

// DBTITLE 1,Distribución de personas en cada rama de actividad que tengan un nivel de instrucción secundaria según cada etnia
display(data3Etnias.filter($"nivel_de_instruccion" === "06 - Secundaria").groupBy($"rama_actividad").pivot($"etnia").count.orderBy("rama_actividad"))


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

// COMMAND ----------


val dataSinOutliersStdDesvSueldoInd  = dfSueldoInd.where($"ingreso_laboral" > inferiorS && $"ingreso_laboral" < superiorS)
dataSinOutliersStdDesvSueldoInd.describe().show
//

val dataSinOutliersStdDesvSueldoMon  = dfSueldoMon.where($"ingreso_laboral" > inferiorSMon && $"ingreso_laboral" < superiorSMon)
dataSinOutliersStdDesvSueldoMon.describe().show
//

val dataSinOutliersStdDesvSueldoMes  = dfSueldoMes.where($"ingreso_laboral" > inferiorSMes && $"ingreso_laboral" < superiorSMes)
dataSinOutliersStdDesvSueldoMes.describe().show

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

// COMMAND ----------

val dataSinOutliersStdDesvEdadInd = dfEdadInd.where($"edad" > inferiorE && $"edad" < superiorE)
dataSinOutliersStdDesvEdadInd.describe().show
//
val dataSinOutliersStdDesvEdadMon = dfEdadMon.where($"edad" > inferiorEMon && $"edad" < superiorEMon)
dataSinOutliersStdDesvEdadMon.describe().show
//
val dataSinOutliersStdDesvEdadMes = dfEdadMes.where($"edad" > inferiorEMes && $"edad" < superiorEMes)
dataSinOutliersStdDesvEdadMes.describe().show
