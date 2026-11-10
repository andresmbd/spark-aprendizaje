## Diferencia entre una Transformación y una Acción en Spark
+ **Transformación**: Es una operación que crea un nuevo DataFrame a partir de otro (por ejemplo: **filter**, **select**, **withColumn**). Spark no calcula nada en el momento en que escribes una transformación; simplemente anota los pasos en un plan lógico (como una receta de cocina).

+ **Acción**: Es una operación que desencadena la ejecución real del trabajo (por ejemplo: **show()**, **count()**, **write()**, **collect()**). Hasta que no llamas a una acción, Spark no procesa los datos.

## ¿Qué significa que Spark evalúe las operaciones de forma diferida (lazy evaluation)? 

Significa que Spark pospone la ejecución de las transformaciones hasta que se invoca una acción. Esto le permite optimizar todo el plan de ejecución global antes de gastar recursos de procesamiento y memoria.

## ¿Para qué sirven los esquemas y cómo se comportan los valores nulos al leer un CSV?

* **Esquemas (StructType)**: Definen de antemano el nombre y el tipo de dato de cada columna (**String**, **Integer**, **Double**, etc.). Usar un esquema evita que Spark tenga que adivinar los tipos escaneando todo el archivo, lo que acelera la lectura.

* **Valores nulos**: Si un campo está vacío o no coincide con el tipo esperado, Spark asigna por defecto un valor null. Puedes controlar esto o rellenarlo usando funciones como **na.drop()** o **na.fill()**.

## ¿Qué implicaciones tiene agrupar y resumir datos en Spark?

Al hacer un **groupBy** seguido de una agregación (como **sum**, **avg**, **count**), Spark necesita mover los datos que pertenecen al mismo grupo hacia una misma partición en el clúster o disco (un proceso pesado llamado shuffle). Esto consume red y memoria, por lo que debe usarse con conciencia en conjuntos de datos masivos.

## ¿Cuándo tiene sentido reutilizar resultados intermedios y qué costo puede tener?

Primero, los resultados intermedios son los conjuntos de datos (DataFrames, Datasets o RDDs) que se generan a mitad de un pipeline de datos tras aplicar ciertas transformaciones, pero que aún no constituyen la salida final del programa.  

Tiene sentido cuando vas a realizar múltiples acciones sobre el mismo DataFrame transformado (por ejemplo, calcular un reporte, guardarlo y luego filtrarlo de nuevo). Usando .cache() o .persist(), evitas que Spark recalculé todo el árbol de transformaciones desde el CSV original. El costo es que ocupa memoria RAM (o disco si se satura la RAM).

## Qué formato de salida elegirías para los resultados y por qué

Para procesamiento analítico moderno, Parquet o Delta son ideales porque almacenan los datos de forma columnar, comprimen muy bien y conservan los tipos de datos. Si necesitas compatibilidad universal para que lo abra cualquier persona en Excel, el formato CSV sigue siendo útil a pesar de no conservar los tipos estrictos.

## ¿Cuándo una solución más sencilla (Python estándar, Pandas o SQL) es preferible a Spark?

- Pandas o Python estándar: Cuando el archivo cabe holgadamente en la memoria RAM de tu computadora (por ejemplo, unos pocos megabytes o un par de millones de filas pequeñas). Spark tiene una sobrecarga inicial (overhead) muy alta de configuración, por lo que para datos pequeños Pandas es más rápido y fácil de escribir.

- SQL tradicional: Si los datos ya viven en una base de datos relacional optimizada, moverlos a Spark solo para consultas simples no tiene sentido.


### Referencias

> 1. https://www.google.com/search?q=https://spark.apache.org/docs/latest/rdd-programming-guide.html%23transformations
> 2. https://spark.apache.org/docs/latest/sql-programming-guide.html
> 3. https://docs.databricks.com/en/optimizations/index.html