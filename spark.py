from pyspark import SparkContext

sc = SparkContext()

with open("ejemplo.txt", 'w') as f:
    f.write("primera linea1\n")
    f.write("segundalinea\n")
    f.write("tercera linea")

textFile = sc.textFile('ejemplo.txt')

print(textFile.collect())