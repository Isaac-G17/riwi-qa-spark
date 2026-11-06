# riwi-qa-spark

Validador de calidad de datos para transacciones financieras construido con **PySpark**, con pruebas automatizadas en **pytest**.

El script lee un archivo CSV de transacciones, aplica reglas de calidad y reporta cada regla como `PASS` o `FAIL`, mostrando los registros problemáticos.

## Reglas de validación

| Regla | Qué detecta |
|---|---|
| Valor inválido | Transacciones con `valor` nulo, cero o negativo |
| Cliente nulo | Transacciones sin `cliente_id` |
| ID duplicado | Transacciones cuyo `id` aparece más de una vez |

## Tecnologías

- Python 3.12
- PySpark 3.5 (sesión local)
- pytest 8

## Estructura

```
riwi-qa-spark/
├── datos/
│   ├── transacciones.csv            # dataset base
│   ├── transacciones_validas.csv    # dataset sin errores
│   └── transacciones_reto.csv       # dataset con errores intencionales
├── docs/
│   └── GUIA_RIWI_QA_PYSPARK_UBUNTU.md
├── src/
│   └── validador.py                 # esquema, reglas y ejecución
├── tests/
│   └── test_validador.py
└── requirements.txt
```

## Requisitos

- Python 3.12
- Java 17 (PySpark lo necesita para ejecutar Spark)

La guía paso a paso para Ubuntu está en [`docs/GUIA_RIWI_QA_PYSPARK_UBUNTU.md`](docs/GUIA_RIWI_QA_PYSPARK_UBUNTU.md).

## Instalación

```bash
git clone https://github.com/Isaac-G17/riwi-qa-spark.git
cd riwi-qa-spark
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Uso

Ejecutar el validador sobre `datos/transacciones_reto.csv`:

```bash
python src/validador.py
```

Para cada regla, la consola muestra la tabla de registros afectados seguida del resultado:

```
=== REGISTROS INVALIDOS ===
+---+----------+-----+
| id|cliente_id|valor|
+---+----------+-----+
|...|       ...|  ...|
FAIL: Se encontraron N transacciones invalidas
```

## Pruebas

```bash
python -m pytest -v
```

Las pruebas verifican que un dataset válido no genere errores y que los valores negativos, nulos y en cero sean detectados correctamente.

## Autor

**Isaac Guzmán** · [GitHub](https://github.com/Isaac-G17) · [Portafolio](https://isaac-g17.github.io)
