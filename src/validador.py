from pathlib import Path 

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from  pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType
)

SCHEMA = StructType([
    StructField('id', IntegerType(), True),
    StructField('cliente_id', IntegerType(), True),
    StructField('valor', LongType(), True),
])


def crear_spark():
    return (
        SparkSession.builder
        .master('local[2]')
        .appName('Riwi-QA-Transacciones')
        .getOrCreate()
    )



def obtener_errores(df):
    """Registros cuyo valor sea nulo, cero o negativo"""
    return df.filter(F.col('valor').isNull() | (F.col('valor') <= 0))


def obtener_error_cliente_nulo(df:DataFrame):
    return df.filter(F.col('cliente_id').isNull())

def obtener_duplicated_id(df:DataFrame):
    ventana = Window.partitionBy('id') # Divide toda la tabla en pequeños grupos o 'islas' basadas en el valor de la columna id
    df_conteo_id = df.withColumn('conteo_id', F.count('id').over(ventana)) # crea una columna y cuenta los id segun las particiones de ventana
    return df_conteo_id.filter(F.col('conteo_id') > 1 ).drop('conteo_id')

def obtener_valor_mayor_limite(df:DataFrame):
    valor_limite = 100000000
    return df.filter(F.col('valor') > valor_limite)

def generar_resumen(df):
    print('\n      RESUMEN DE ERRORES      \n')
    null_id = obtener_error_cliente_nulo(df).count()
    print(f'      ID CLIENTES NULO: {null_id}')

    id_duplicados = obtener_duplicated_id(df).count()
    print(f'      ID DUPLICADOS: {id_duplicados}')

    valores_superados = obtener_valor_mayor_limite(df).count()
    print(f'      VALORES SUPERANDO LIMITE: {valores_superados}') 


def main():
    spark = crear_spark()
    try: # __file__ es una variable especial (un atributo nativo) que contiene la ruta del archivo actual que se está ejecutando.
        ruta = Path(__file__).resolve().parents[1] /'datos'/'transacciones_reto.csv'
        df: DataFrame = (
            spark.read
            .option('header', True)
            .schema(SCHEMA)
            .csv(str(ruta))
        )
        print("\n     DATOS ORIGINALES     \n")
        df.show()

        errores = obtener_errores(df)
        print("\n     REGISTROS INVALIDOS     \n")
        errores.show()

        total = errores.count()

        if total == 0:
            print('PASS: No hay transacciones invalidas')
        else:
            print(f'FAIL: se encontraron {total} transacciones invalidas')

        generar_resumen(df)


    finally:
        spark.stop()

if __name__ == '__main__':
    main()

