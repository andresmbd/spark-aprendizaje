import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable #  variable interna que almacena la ruta absoluta exacta del archivo python.exe
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
"""
INTRODUCCION
METODOS DE ACCION
"""

from pyspark import SparkContext

sc = SparkContext()

with open("ejemplo.txt", 'w') as f:
    f.write("primera linea1\n")
    f.write("segundalinea\n")
    f.write("tercera linea")

textFile = sc.textFile('ejemplo.txt')

print(textFile.collect())
count = textFile.count()
print(count)
