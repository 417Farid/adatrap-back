#from django.shortcuts import render
import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, lag, avg
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from django.http import JsonResponse

spark = SparkSession.builder \
  .appName("Datos") \
  .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
  .getOrCreate()

fullData = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/inkco") \
    .option("query", "SELECT td.id_despacho, td.id_ruta, tr.nombre, td.fecha_inicio, td.fecha_fin, td.pasajeros, tt.velocidad FROM t_despacho td JOIN t_track tt ON td.id_despacho = tt.id_track JOIN t_ruta tr ON td.id_ruta = tr.id_ruta") \
    .option("user", "root") \
    .option("password", "nomelase") \
    .load()

# ------------------------------------------------------------------------------------------------------------------------

# OPERACIONES PARA LA FRECUENCIA

# ------------------------------------------------------------------------------------------------------------------------

# Convierte la columna 'fecha_inicio' a formato timestamp
df = fullData.withColumn("fecha_inicio", F.to_timestamp("fecha_inicio", "yyyy-MM-dd HH:mm:ss"))

# Ordena el DataFrame por fecha_inicio de menor a mayor
df = fullData.orderBy("fecha_inicio")

# Calcular la diferencia en minutos entre filas consecutivas
windowSpec = Window().orderBy("fecha_inicio")
df = df.withColumn("diferencia_minutos", (F.unix_timestamp("fecha_inicio") - F.unix_timestamp(F.lag("fecha_inicio").over(windowSpec))) / 60)

# Crear columnas adicionales para el mes, hora, día de la semana
df = df.withColumn("mes", F.month("fecha_inicio"))
df = df.withColumn("hora", F.hour("fecha_inicio"))
df = df.withColumn("dia_semana", F.dayofweek("fecha_inicio"))

# Crear DataFrames para cada grupo de resultados solicitado
promedio_mes = df.groupBy("mes").agg(F.avg("diferencia_minutos").alias("promedio_diferencia_mes"))
promedio_hora = df.groupBy("hora").agg(F.avg("diferencia_minutos").alias("promedio_diferencia_hora"))
promedio_dia_semana = df.groupBy("dia_semana").agg(F.avg("diferencia_minutos").alias("promedio_diferencia_dia_semana"))
promedio_enero_febrero = df.filter((F.col("mes").isin([1, 2]))).agg(F.avg("diferencia_minutos").alias("promedio_diferencia_enero_febrero"))
promedio_por_ruta = df.groupBy("id_ruta", "nombre").agg(F.avg("diferencia_minutos").alias("promedio_diferencia_ruta"))

# Eliminar la columna "id_ruta"
columna_a_eliminar = "id_ruta"
df_sin_columna = promedio_por_ruta.drop(columna_a_eliminar)

# ------------------------------------------------------------------------------------------------------------------------

# OPERACIONES PARA EL TIEMPO

# ------------------------------------------------------------------------------------------------------------------------

# Convierte la columna 'fecha_inicio' a formato timestamp
df = df.withColumn("fecha_inicio", F.to_timestamp("fecha_inicio", "yyyy-MM-dd HH:mm:ss"))

# Calcular la diferencia en minutos entre filas consecutivas
windowSpec = Window().orderBy("fecha_inicio")
df = df.withColumn("diferencia_minutos", (F.unix_timestamp("fecha_inicio") - F.unix_timestamp(F.lag("fecha_inicio").over(windowSpec))) / 60)

# Calcular el promedio de velocidad por ruta
promedio_velocidad_ruta = df.groupBy("id_ruta", "nombre").agg(F.avg("velocidad").alias("promedio_velocidad_ruta"))

# Calcular el promedio de velocidad por día de la semana
promedio_velocidad_dia_semana = df.groupBy("dia_semana").agg(F.avg("velocidad").alias("promedio_velocidad_dia_semana"))

# Obtener las horas del día con más velocidad
horas_mas_velocidad = df.groupBy("hora").agg(F.avg("velocidad").alias("promedio_velocidad_hora")).orderBy(F.desc("promedio_velocidad_hora")).limit(5)

# Obtener las 5 rutas con mayor velocidad
rutas_mas_velocidad = df.groupBy("id_ruta", "nombre").agg(F.avg("velocidad").alias("promedio_velocidad_ruta")).orderBy(F.desc("promedio_velocidad_ruta")).limit(5)

# Obtener los 5 meses del año con mayor velocidad
meses_mas_velocidad = df.groupBy("mes").agg(F.avg("velocidad").alias("promedio_velocidad_mes")).orderBy(F.desc("promedio_velocidad_mes")).limit(5)

# ------------------------------------------------------------------------------------------------------------------------

def mesFrecuencia(request):
    data = [json.loads(line) for line in promedio_mes.toJSON().collect()]
    return JsonResponse(data, safe=False)

def horaFrecuencia(request):
    data = [json.loads(line) for line in promedio_hora.toJSON().collect()]
    return JsonResponse(data, safe=False)

def diaFrecuencia(request):
    data = [json.loads(line) for line in promedio_dia_semana.toJSON().collect()]
    return JsonResponse(data, safe=False)

def vacasFrecuencia(request):
    data = [json.loads(line) for line in promedio_enero_febrero.toJSON().collect()]
    return JsonResponse(data, safe=False)

def rutaFrecuencia(request):
    data = [json.loads(line) for line in df_sin_columna.toJSON().collect()]
    return JsonResponse(data, safe=False)

# ------------------------------------------------------------------------------------------------------------------------

def rutasVelocidad(request):
    data = [json.loads(line) for line in promedio_velocidad_ruta.toJSON().collect()]
    return JsonResponse(data, safe=False)

def horaVelocidad(request):
    data = [json.loads(line) for line in horas_mas_velocidad.toJSON().collect()]
    return JsonResponse(data, safe=False)

def cincoRutVelocidad(request):
    data = [json.loads(line) for line in rutas_mas_velocidad.toJSON().collect()]
    return JsonResponse(data, safe=False)

def mesVelocidad(request):
    data = [json.loads(line) for line in meses_mas_velocidad.toJSON().collect()]
    return JsonResponse(data, safe=False)

def diaVelocidad(request):
    data = [json.loads(line) for line in promedio_velocidad_dia_semana.toJSON().collect()]
    return JsonResponse(data, safe=False)



   