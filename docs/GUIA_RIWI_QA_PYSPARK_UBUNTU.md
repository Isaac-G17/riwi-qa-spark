# RIWI | Laboratorio de complementos: QA de datos con Python y Apache Spark

**Nivel:** inicial  
**Duración estimada:** 90–120 minutos, más reto autónomo  
**Sistema:** Ubuntu 22.04 / 24.04 (Linux, instalación local)  
**Costo:** gratuito; no requiere AWS, Google Cloud, API keys ni tarjeta de crédito.

## 1. Propósito

Aprender a instalar Apache Spark mediante PySpark, leer un archivo CSV, identificar registros que incumplen reglas de negocio y automatizar las verificaciones con Pytest. El caso simula transacciones financieras con **datos completamente ficticios**.

### ¿Por qué Spark?

Apache Spark permite procesar datos en paralelo y, cuando se configura sobre un clúster, distribuir el trabajo entre varias máquinas. Se utiliza, por ejemplo, para comprobar calidad en grandes conjuntos de transacciones, comparar datos de origen y destino de un ETL, o verificar millones de registros históricos. **Este laboratorio usa Spark en modo local con siete registros solo para aprender:** con tan pocos datos, Python o pandas serían más simples y probablemente más rápidos.

| Escenario | ¿Usar Spark? | Razón |
|---|---|---|
| Revisar 7 registros | No es necesario | Python basta |
| Validar un JSON pequeño de una API | No | Python + Pytest |
| Analizar 50 millones de transacciones | Posiblemente sí | Procesamiento paralelo y escalabilidad |
| Reconciliar grandes tablas de un pipeline ETL | Frecuentemente sí | Joins y agregaciones a escala |
| Procesar archivos distribuidos en un clúster | Sí, si el caso lo justifica | Trabajo repartido entre nodos |

## 2. Antes de empezar

- Acceso a una terminal de Ubuntu y permiso para instalar paquetes con `sudo`.
- Conexión a internet para la instalación inicial.
- Al menos 4 GB de RAM para este laboratorio; 8 GB recomendados.
- Conocimientos básicos de funciones, listas y condicionales en Python.

> **Compatibilidad:** para mantener el ejercicio reproducible, usamos **PySpark 3.5.6 + Java 17** y una versión de Python compatible con Spark 3.5 (por ejemplo, Python 3.10, 3.11 o 3.12). Si `python3 --version` muestra Python 3.13 o posterior, usa un intérprete compatible antes de continuar. Ubuntu 22.04 normalmente incluye Python 3.10 y Ubuntu 24.04, Python 3.12.

## 3. Instalación paso a paso

### Paso 1. Actualizar paquetes e instalar herramientas

Abre la terminal (`Ctrl + Alt + T`) y ejecuta:

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv openjdk-17-jdk git curl
```

Verifica las versiones:

```bash
python3 --version
java -version
```

Debes observar Python compatible y OpenJDK 17. Si tienes varias versiones de Java, identifica la ruta activa:

```bash
readlink -f "$(command -v java)"
```

En Ubuntu x86_64 con Java 17 instalado mediante APT, puedes seleccionar Java 17 para la sesión:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
java -version
```

> En otras arquitecturas, ajusta `JAVA_HOME` según la ruta real de instalación. Esta configuración solo dura mientras la terminal esté abierta.

### Paso 2. Crear carpeta y entorno virtual

```bash
mkdir -p ~/riwi-qa-spark
cd ~/riwi-qa-spark
python3 -m venv .venv
source .venv/bin/activate
```

Verás `(.venv)` al inicio de la línea de comandos. Comprueba que se usa Python del entorno:

```bash
which python
python --version
```

### Paso 3. Instalar dependencias

```bash
python -m pip install --upgrade pip
python -m pip install 'pyspark==3.5.6' 'pytest>=8,<9'
python -c 'import pyspark; print("PySpark:", pyspark.__version__)'
```

La última línea debe mostrar `PySpark: 3.5.6`.

### Paso 4. Probar el motor Spark

```bash
python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[2]').appName('Riwi-Prueba').getOrCreate()
print('Spark funcionando. Version:', spark.version)
spark.stop()
PY
```

Es normal ver mensajes informativos de Java o Spark. Lo importante es que aparezca `Spark funcionando` y que el proceso termine sin excepción.

## 4. Estructura del proyecto

Crea los directorios:

```bash
mkdir -p datos src tests
touch src/__init__.py
```

La estructura final será:

```text
riwi-qa-spark/
├── .venv/                    # Entorno local; no subir a Git
├── datos/
│   └── transacciones.csv
├── src/
│   ├── __init__.py
│   └── validador.py
├── tests/
│   └── test_validador.py
├── requirements.txt
└── .gitignore
```

Crea los archivos auxiliares:

```bash
cat > requirements.txt <<'EOF_REQ'
pyspark==3.5.6
pytest>=8,<9
EOF_REQ

cat > .gitignore <<'EOF_GIT'
.venv/
__pycache__/
.pytest_cache/
*.pyc
EOF_GIT
```

## 5. Crear un CSV con errores intencionales

Desde `~/riwi-qa-spark` ejecuta:

```bash
cat > datos/transacciones.csv <<'EOF_CSV'
id,cliente_id,valor
1,101,50000
2,102,-20000
3,103,150000
4,104,
5,105,0
6,106,85000
7,107,70000
EOF_CSV
```

Las reglas de negocio **ficticias** son:

1. `valor` es obligatorio.
2. `valor` debe ser mayor que cero.

Antes de programar, identifica manualmente los registros problemáticos: **2 (negativo), 4 (vacío), 5 (cero)**.

## 6. Implementar el validador de calidad de datos

Crea `src/validador.py` con el siguiente contenido completo:

```python
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
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
    """Registros cuyo valor es nulo, cero o negativo."""
    return df.filter(col('valor').isNull() | (col('valor') <= 0))


def main():
    spark = crear_spark()
    try:
        ruta = Path(__file__).resolve().parents[1] / 'datos' / 'transacciones.csv'
        df = (
            spark.read
            .option('header', True)
            .schema(SCHEMA)
            .csv(str(ruta))
        )

        print('\n=== DATOS ORIGINALES ===')
        df.show()

        errores = obtener_errores(df)
        print('\n=== REGISTROS INVALIDOS ===')
        errores.show()

        total = errores.count()
        if total == 0:
            print('PASS: No hay transacciones invalidas')
        else:
            print(f'FAIL: Se encontraron {total} transacciones invalidas')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
```

> **Nota QA:** aquí `FAIL` es el **resultado de calidad del archivo**, no un fallo de instalación ni un error del código. En una solución empresarial, las reglas y los datos se definen con el área de negocio.

### Ejecutar el validador

Desde la raíz del proyecto:

```bash
python -m src.validador
```

Salida esencial esperada (Spark puede mostrar otros mensajes):

```text
=== REGISTROS INVALIDOS ===
+---+----------+------+
| id|cliente_id| valor|
+---+----------+------+
|  2|       102|-20000|
|  4|       104|  NULL|
|  5|       105|     0|
+---+----------+------+

FAIL: Se encontraron 3 transacciones invalidas
```

**Preguntas:** ¿Por qué `None`/vacío requiere `isNull()`? ¿Qué hace `filter()`? ¿Qué significa `count()`? ¿Por qué no corregimos automáticamente el valor negativo?

## 7. Automatizar la comprobación con Pytest

Crea `tests/test_validador.py`:

```python
import pytest

from src.validador import SCHEMA, crear_spark, obtener_errores


@pytest.fixture(scope='module')
def spark():
    sesion = crear_spark()
    yield sesion
    sesion.stop()


def test_datos_validos_no_generan_errores(spark):
    datos = [(1, 101, 50000), (2, 102, 10000), (3, 103, 75000)]
    df = spark.createDataFrame(datos, SCHEMA)
    assert obtener_errores(df).count() == 0


def test_datos_invalidos_son_detectados(spark):
    datos = [(1, 101, 50000), (2, 102, -20000),
             (3, 103, None), (4, 104, 0)]
    df = spark.createDataFrame(datos, SCHEMA)
    errores = obtener_errores(df)

    assert errores.count() == 3
    # collect() solo es adecuado aquí porque el dataset es diminuto.
    ids = {fila['id'] for fila in errores.select('id').collect()}
    assert ids == {2, 3, 4}
```

Ejecuta:

```bash
python -m pytest -v
```

Resultado esperado:

```text
test_datos_validos_no_generan_errores PASSED
test_datos_invalidos_son_detectados PASSED
2 passed
```

**Importante:** el archivo de datos puede reprobar la validación (`FAIL: 3`) y las pruebas de Pytest pueden pasar (`2 passed`) simultáneamente. Las pruebas pasan porque el código detecta correctamente los errores que se sembraron.

## 8. Experimento: comprobar un caso sin errores

Crea otro CSV con todos los valores positivos:

```bash
cat > datos/transacciones_validas.csv <<'EOF_OK'
id,cliente_id,valor
1,101,50000
2,102,20000
3,103,150000
4,104,10000
5,105,30000
6,106,85000
7,107,70000
EOF_OK
```

Para probarlo, cambia temporalmente en `src/validador.py` el nombre `transacciones.csv` por `transacciones_validas.csv` y ejecuta:

```bash
python -m src.validador
```

Ahora el resultado debe ser:

```text
PASS: No hay transacciones invalidas
```

Devuelve el nombre del archivo original al terminar.

## 9. Reto autónomo: ampliar el control de calidad

Sin copiar una solución del instructor, agrega estas reglas:

- **R1:** `cliente_id` no puede ser nulo.
- **R2:** `id` de transacción no puede repetirse.
- **R3:** `valor` no puede superar `100000000` (límite ficticio para el ejercicio).
- **R4:** generar un resumen con cantidad de errores por regla.
- **R5:** crear al menos una prueba automatizada por cada regla nueva.

### Datos adicionales para probar las nuevas reglas

Puedes crear otro archivo:

```bash
cat > datos/transacciones_reto.csv <<'EOF_RETO'
id,cliente_id,valor
1,101,50000
2,102,20000
2,103,35000
3,,80000
4,104,150000000
5,105,0
6,106,
EOF_RETO
```

**Pistas técnicas:** `col('cliente_id').isNull()`, `groupBy('id').count()`, `filter(col('count') > 1)`. Ten presente que un registro puede incumplir más de una regla; distingue entre **cantidad de errores** y **cantidad de filas inválidas**.

### Criterios de evaluación

| Criterio | Peso |
|---|---:|
| Instalación y ejecución local | 15 % |
| Detección de errores con Spark | 25 % |
| Reglas de calidad adicionales | 25 % |
| Pruebas automatizadas con Pytest | 20 % |
| Explicación de cuándo usar Spark | 15 % |
| **Total** | **100 %** |

## 10. Reflexión: utilidad real de Spark

Responde en tu `README.md`:

1. ¿Qué diferencia hay entre un DataFrame de Spark y una lista de Python?
2. ¿Por qué Spark no es necesario para siete registros?
3. ¿Qué cambia cuando se deben validar decenas de millones de transacciones?
4. ¿Qué es una validación de datos y qué es una prueba automatizada del validador?
5. ¿Qué problemas puede causar una transacción duplicada en un pipeline financiero?
6. ¿Qué controles aplicarías para comparar la cantidad de registros de entrada y salida de un ETL?

## 11. Solución de problemas frecuentes

| Mensaje o síntoma | Acción |
|---|---|
| `python: command not found` | Activa `.venv` y verifica `python3 --version`. |
| `No module named pyspark` | Ejecuta `source .venv/bin/activate` y reinstala `requirements.txt`. |
| `java: command not found` | Instala `openjdk-17-jdk` y revisa `java -version`. |
| Error de `JAVA_HOME` | Comprueba la ruta real de Java 17. |
| Error al iniciar el proceso Java | Revisa versiones de Java/Python, memoria y el mensaje completo del log. |
| `ModuleNotFoundError: No module named 'src'` | Ejecuta `python -m pytest -v` desde `~/riwi-qa-spark`. |
| CSV no encontrado | Ejecuta el proyecto con la estructura indicada y verifica `datos/transacciones.csv`. |
| Advertencias de Spark al arrancar | Algunas son informativas; verifica si se imprime el resultado y termina correctamente. |

Para retomar el laboratorio otro día:

```bash
cd ~/riwi-qa-spark
source .venv/bin/activate
python -m src.validador
python -m pytest -v
```

Si Java 17 no es el Java predeterminado de tu terminal, vuelve a configurar `JAVA_HOME` como en el paso 1.

## 12. Entregables

Sube a tu repositorio de Git (sin `.venv`) lo siguiente:

1. `src/validador.py` con las reglas originales y las del reto.
2. `tests/test_validador.py` con las pruebas automatizadas.
3. `datos/transacciones.csv` y `datos/transacciones_reto.csv` (solo datos sintéticos).
4. `requirements.txt` y `.gitignore`.
5. `README.md` con instrucciones de ejecución, resultados de las pruebas, capturas o salidas y respuestas de reflexión.

**Criterio de finalización:** el coder demuestra que puede instalar PySpark gratis en Ubuntu, detectar los errores intencionales, ejecutar Pytest y explicar cuándo Spark aporta valor frente a soluciones más sencillas.

## 13. Referencias

- [Documentación de Apache Spark 3.5.6](https://spark.apache.org/docs/3.5.6/)
- [Instalación de PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/getting_started/install.html)
- [Documentación de Pytest](https://docs.pytest.org/en/stable/)
- [Python venv](https://docs.python.org/3/library/venv.html)

---

**Riwi — Ruta de complementos | QA con Python + Apache Spark**
