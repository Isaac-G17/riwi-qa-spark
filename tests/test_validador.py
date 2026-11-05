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