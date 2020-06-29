// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
val myDataSchema = StructType(
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

// DBTITLE 1,Creación del DataFrame asignándole el esquema creado anteriormente
val data = spark
  .read
  .schema(myDataSchema)
  .option("header","true")
  .option("delimiter", "\t")
  .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------

// DBTITLE 1,Extracción de las columnas de interés a trabajar de acuerdo a cada Etnia
val dataInd = data.select("etnia", "grupo_ocupacion", "nivel_de_instruccion", "ingreso_laboral", "rama_actividad", "sectorizacion","genero").where($"etnia" === "1 - Indígena")
val dataAfr = data.select("etnia", "grupo_ocupacion", "nivel_de_instruccion", "ingreso_laboral", "rama_actividad", "sectorizacion","genero").where($"etnia" === "2 - Afroecuatoriano")
val dataNeg = data.select("etnia", "grupo_ocupacion", "nivel_de_instruccion", "ingreso_laboral", "rama_actividad", "sectorizacion", "genero").where($"etnia" === "3 - Negro")
val dataMul = data.select("etnia", "grupo_ocupacion", "nivel_de_instruccion", "ingreso_laboral", "rama_actividad", "sectorizacion", "genero").where($"etnia" === "4 - Mulato")
val dataMon = data.select("etnia", "grupo_ocupacion", "nivel_de_instruccion", "ingreso_laboral", "rama_actividad", "sectorizacion","genero").where($"etnia" === "5 - Montubio")
val dataMes = data.select("etnia", "grupo_ocupacion", "nivel_de_instruccion", "ingreso_laboral", "rama_actividad", "sectorizacion","genero").where($"etnia" === "6 - Mestizo")
val dataBla = data.select("etnia", "grupo_ocupacion", "nivel_de_instruccion", "ingreso_laboral", "rama_actividad", "sectorizacion","genero").where($"etnia" === "7 - Blanco")
val dataOtr = data.select("etnia", "grupo_ocupacion", "nivel_de_instruccion", "ingreso_laboral", "rama_actividad", "sectorizacion","genero").where($"etnia" === "8 - Otro")


// COMMAND ----------

// DBTITLE 1,Porcentaje donde Ingreso Laboral sea menor al salario básico, de acuerdo a cada Etnia
println(f"${(dataInd.where($"ingreso_laboral" < 400).count / dataInd.count.toDouble) * 100}%.2f%% Indigenas")
println(f"${(dataAfr.where($"ingreso_laboral" < 400).count / dataAfr.count.toDouble) * 100}%.2f%% Afroecuatoriano")
println(f"${(dataNeg.where($"ingreso_laboral" < 400).count / dataNeg.count.toDouble) * 100}%.2f%% Negro")
println(f"${(dataMul.where($"ingreso_laboral" < 400).count / dataMul.count.toDouble) * 100}%.2f%% Mulato")
println(f"${(dataMon.where($"ingreso_laboral" < 400).count / dataMon.count.toDouble) * 100}%.2f%% Montubio")
println(f"${(dataMes.where($"ingreso_laboral" < 400).count / dataMes.count.toDouble) * 100}%.2f%% Mestizo")
println(f"${(dataBla.where($"ingreso_laboral" < 400).count / dataBla.count.toDouble) * 100}%.2f%% Blanco")
println(f"${(dataOtr.where($"ingreso_laboral" < 400).count / dataOtr.count.toDouble) * 100}%.2f%% Otro")

// COMMAND ----------

// DBTITLE 1,Porcentaje donde el campo Ingreso Laboral sea Nulo , de acuerdo a cada Etnia
println(f"${(dataInd.where($"ingreso_laboral".isNull).count / dataInd.count.toDouble) * 100}%.2f%% Indigenas")
println(f"${(dataAfr.where($"ingreso_laboral".isNull).count / dataAfr.count.toDouble) * 100}%.2f%% Afroecuatoriano")
println(f"${(dataNeg.where($"ingreso_laboral".isNull).count / dataNeg.count.toDouble) * 100}%.2f%% Negro")
println(f"${(dataMul.where($"ingreso_laboral".isNull).count / dataMul.count.toDouble) * 100}%.2f%% Mulato")
println(f"${(dataMon.where($"ingreso_laboral".isNull).count / dataMon.count.toDouble) * 100}%.2f%% Montubio")
println(f"${(dataMes.where($"ingreso_laboral".isNull).count / dataMes.count.toDouble) * 100}%.2f%% Mestizo")
println(f"${(dataBla.where($"ingreso_laboral".isNull).count / dataBla.count.toDouble) * 100}%.2f%% Blanco")
println(f"${(dataOtr.where($"ingreso_laboral".isNull).count / dataOtr.count.toDouble) * 100}%.2f%% Otro")

// COMMAND ----------

// DBTITLE 1,¿Cuales son las 5 ramas de actividad con mas personas de cada Etnia?
dataInd.groupBy($"rama_actividad".as ("Etnia Indigena - Rama Actividad")).count.sort(desc("count")).show(numRows= 5, truncate = false)
dataAfr.groupBy($"rama_actividad".as ("Etnia Afroecuatoriano - Rama Actividad")).count.sort(desc("count")).show(numRows= 5, truncate = false)
dataNeg.groupBy($"rama_actividad".as ("Etnia Negra - Rama Actividad")).count.sort(desc("count")).show(numRows= 5, truncate = false)
dataMul.groupBy($"rama_actividad".as ("Etnia Mulato - Rama Actividad")).count.sort(desc("count")).show(numRows= 5, truncate = false)
dataMon.groupBy($"rama_actividad".as ("Etnia Montubio - Rama Actividad")).count.sort(desc("count")).show(numRows= 5, truncate = false)
dataMes.groupBy($"rama_actividad".as ("Etnia Mestizo - Rama Actividad")).count.sort(desc("count")).show(numRows= 5, truncate = false)
dataBla.groupBy($"rama_actividad".as ("Etnia Blanco - Rama Actividad")).count.sort(desc("count")).show(numRows= 5, truncate = false)
dataOtr.groupBy($"rama_actividad".as ("Otro - Rama Actividad")).count.sort(desc("count")).show(numRows= 5, truncate = false)

// COMMAND ----------

// DBTITLE 1,¿Cual es la cantidad de personas en cada rama de actividad de aquellos ubicados en el sector informal pertenecientes a cada etnia?
dataInd.filter($"sectorizacion" === "2 - Sector Informal").groupBy($"rama_actividad". as(" Ramas Actividad - Etnia Indigena")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataAfr.filter($"sectorizacion" === "2 - Sector Informal").groupBy($"rama_actividad". as(" Ramas Actividad - Etnia Indigena")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataNeg.filter($"sectorizacion" === "2 - Sector Informal").groupBy($"rama_actividad". as(" Ramas Actividad - Etnia Indigena")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataMul.filter($"sectorizacion" === "2 - Sector Informal").groupBy($"rama_actividad". as(" Ramas Actividad - Etnia Indigena")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataMon.filter($"sectorizacion" === "2 - Sector Informal").groupBy($"rama_actividad". as(" Ramas Actividad - Etnia Indigena")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataMes.filter($"sectorizacion" === "2 - Sector Informal").groupBy($"rama_actividad". as(" Ramas Actividad - Etnia Indigena")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataBla.filter($"sectorizacion" === "2 - Sector Informal").groupBy($"rama_actividad". as(" Ramas Actividad - Etnia Indigena")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataOtr.filter($"sectorizacion" === "2 - Sector Informal").groupBy($"rama_actividad". as(" Ramas Actividad - Etnia Indigena")).count.sort(desc("count")).show(numRows= 8, truncate = false)

// COMMAND ----------

// DBTITLE 1,¿Cuantas personas que tengan un nivel de instrucción primaria se ubican en cada rama de actividad según su etnia?
dataInd.filter($"nivel_de_instruccion" === "04 - Primaria").groupBy($"rama_actividad".as("Etnia Indigena - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataAfr.filter($"nivel_de_instruccion" === "04 - Primaria").groupBy($"rama_actividad".as("Etnia Afroecuatoriano - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataNeg.filter($"nivel_de_instruccion" === "04 - Primaria").groupBy($"rama_actividad".as("Etnia Negro - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataMul.filter($"nivel_de_instruccion" === "04 - Primaria").groupBy($"rama_actividad".as("Etnia Mulato - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataMon.filter($"nivel_de_instruccion" === "04 - Primaria").groupBy($"rama_actividad".as("Etnia Montubio - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataMes.filter($"nivel_de_instruccion" === "04 - Primaria").groupBy($"rama_actividad".as("Etnia Mestizo - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataBla.filter($"nivel_de_instruccion" === "04 - Primaria").groupBy($"rama_actividad".as("Etnia Blanco - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataOtr.filter($"nivel_de_instruccion" === "04 - Primaria").groupBy($"rama_actividad".as("Otro - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)

// COMMAND ----------

// DBTITLE 1,¿Cuantas personas que tengan un nivel de instrucción secundaria se ubican en cada rama de actividad según su etnia?
dataInd.filter($"nivel_de_instruccion" === "06 - Secundaria").groupBy($"rama_actividad".as("Etnia Indigena - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataAfr.filter($"nivel_de_instruccion" === "06 - Secundaria").groupBy($"rama_actividad".as("Etnia Afroecuatoriano - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataNeg.filter($"nivel_de_instruccion" === "06 - Secundaria").groupBy($"rama_actividad".as("Etnia Negro - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataMul.filter($"nivel_de_instruccion" === "06 - Secundaria").groupBy($"rama_actividad".as("Etnia Mulato - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataMon.filter($"nivel_de_instruccion" === "06 - Secundaria").groupBy($"rama_actividad".as("Etnia Montubio - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataMes.filter($"nivel_de_instruccion" === "06 - Secundaria").groupBy($"rama_actividad".as("Etnia Mestizo - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataBla.filter($"nivel_de_instruccion" === "06 - Secundaria").groupBy($"rama_actividad".as("Etnia Blanco - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)
dataOtr.filter($"nivel_de_instruccion" === "06 - Secundaria").groupBy($"rama_actividad".as("Otro - Rama Actividad")).count.sort(desc("count")).show(numRows= 8, truncate = false)

