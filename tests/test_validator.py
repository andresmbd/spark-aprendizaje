import pytest
from pyspark.sql import SparkSession
from validator import cargas_datos

"""
En pytest, una fixture es una función decorada que 
sirve para preparar un entorno o recurso controlado 
antes de ejecutar las pruebas (por ejemplo, conectar 
a una base de datos, cargar un archivo de configuración, 
o en nuestro caso, levantar una sesión de Spark).
"""

"""
las fixtures de pytest se ejecutan cada vez que corre una 
prueba individual (scope="function").

Levantar una sesión de Spark requiere inicializar la JVM, asignar memoria, etc. 
lo cual puede tomar varios segundos.
"""
@pytest.fixture(scope='session') # "Levanta una sola sesión de Spark al principio de toda la sesión de pruebas, compártela entre todos los archivos de test, y ciérrala únicamente al terminar absolutamente todas las pruebas
def spark_session():
    spark = SparkSession.builder\
        .appName('ValidatorSpark')\
        .master('local[2]')\
        .getOrCreate()
    """
    En Python, normalmente usamos return para devolver un valor en una función. Pero las fixtures necesitan hacer dos cosas:
        1. Configurar algo antes de la prueba (crear la sesión de Spark).
        2. Limpiar / Cerrar los recursos después de que terminan las pruebas (apagar Spark para liberar memoria).
    """
    # Funciona como un return temporal: le entrega el objeto spark a la prueba para que lo use.
    # Pausa la ejecución de la fixture mientras corren las pruebas.
    # Justo cuando todas las pruebas terminan, la ejecución de la función de la fixture se reanuda en la línea siguiente al yield, permitiendo ejecutar spark.stop().
    yield spark

    spark.stop()