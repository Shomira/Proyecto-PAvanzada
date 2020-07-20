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
