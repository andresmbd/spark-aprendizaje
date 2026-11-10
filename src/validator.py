from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, DoubleType, LongType


def inizializar_spark():
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



















