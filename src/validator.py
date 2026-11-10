from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, LongType
from pyspark.sql import functions as F
from pathlib import Path


def inizializar_spark():
    """Inicializa y retorna la sesión de Spark."""
    return (SparkSession.builder
        .master('local[*]') # configura Spark para ejecutarse localmente en tu computadora, sin conectarse a un clúster. El * indica que usará todos los núcleos de CPU disponibles para procesar tareas en paralelo.
        .appName('ValidatorTransactionSpark')
        .getOrCreate()
    )


def cargas_datos(spark, ruta_csv):
    
    
    """ representa la estructura completa de la tabla.
Cada StructField(...) define una columna"""
    schema = StructType([
        StructField('id', LongType(), True), # el 3er @arg indica que la columna puede contener valores nulos. 
        StructField('cliente_id', LongType(), True),
        StructField('valor', DoubleType(), True),
    ])

    df = (
        spark.read
        .option('header', True) # indica a Spark que la primera fila del CSV contiene los nombres de las columnas
        .schema(schema)
        .csv(str(ruta_csv))
    )

    return df


def generar_resumen(df_validos):
    resumen = df_validos.groupBy('cliente_id') \
        .agg(
            F.count('id').alias('Total_transacciones'),
            F.sum('valor').alias('Valor_total'),
            F.avg('valor').alias('Valor_promedio')
        )
    return resumen

def main():
    spark = inizializar_spark()
    ruta = Path(__file__).resolve().parent[1]/'data'/'transacciones_practica.csv'

    df_raw = cargas_datos(spark, ruta)
    df_raw.show()
    spark.stop()



main()














