#from django.shortcuts import render
import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, lag, avg
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from rest_framework import viewsets
from django.http import JsonResponse

spark = SparkSession.builder \
  .appName("Datos") \
  .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
  .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/inkco") \
    .option("query", "SELECT td.id_despacho, td.id_ruta, tr.nombre, td.fecha_inicio, td.fecha_fin, td.pasajeros, tt.velocidad FROM t_despacho td JOIN t_track tt ON td.id_despacho = tt.id_track JOIN t_ruta tr ON td.id_ruta = tr.id_ruta") \
    .option("user", "root") \
    .option("password", "nomelase") \
    .load()

def prueba(request):
    frecuencia_df = df.select("id_ruta", "nombre", "fecha_inicio")
    data = [json.loads(line) for line in frecuencia_df.toJSON().collect()]
    return JsonResponse(data, safe=False)
    #return type(data)