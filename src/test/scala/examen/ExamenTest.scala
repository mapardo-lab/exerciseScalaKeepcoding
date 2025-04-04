package examen

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import examen.Examen._
import utils.TestInit

class ExamenTest extends TestInit {

  val sc = spark.sparkContext

  val schemaEstudiantes = StructType(Seq(
    StructField("nombre", StringType, nullable = false),
    StructField("edad", IntegerType, nullable = false),
    StructField("calificacion", DoubleType, nullable = false)
  ))

  val tuplasEstudiantes: Seq[(String, Int, Double)] =
    Seq(("Juan", 35, 3),
      ("Pepe", 23, 8),
      ("Lucas", 27, 8.5),
      ("Paco", 50, 4),
      ("Miguel", 45, 3.2),
      ("Ruben", 33, 5.8),
      ("Sofia", 27, 8.8),
      ("David", 37, 9),
    )

  "ejercicio1" should "crear y mortrar dataframe" in {

    val dfEstudiantes: DataFrame = newDf(tuplasEstudiantes.map(Row.fromTuple), schemaEstudiantes)

    dfEstudiantes.printSchema()

    dfEstudiantes.show()
  }

  "ejercicio1" should "filtrar por calificación" in {

    val dfEstudiantes: DataFrame = newDf(tuplasEstudiantes.map(Row.fromTuple), schemaEstudiantes)

    val dfOut: DataFrame = ejercicio1a(dfEstudiantes)

    dfOut.show()
  }

  "ejercicio1" should "ordenar nombres según calificación" in {

    val dfEstudiantes: DataFrame = newDf(tuplasEstudiantes.map(Row.fromTuple), schemaEstudiantes)

    val dfOut: DataFrame = ejercicio1b(dfEstudiantes)

    dfOut.show()
  }

  "ejercicio2" should "reconocer números negativos" in {
    val schemaTemperaturas = StructType(Seq(
      StructField("ciudad", StringType, nullable = false),
      StructField("temperaturaMinima", DoubleType, nullable = false)
    ))

    val tuplasTemperaturas: Seq[(String, Double)] =
      Seq(("Pekin", 12.1),
        ("Madrid", 5.0),
        ("Estocolmo", -10.3),
        ("Barcelona", 27.6),
        ("Oslo", -5.2),
        ("Paris", 0.0)
      )

    val dfTemperaturas: DataFrame = newDf(tuplasTemperaturas.map(Row.fromTuple), schemaTemperaturas)

    val dfOut: DataFrame = ejercicio2(dfTemperaturas)

    dfOut.show()


  }

  "ejercicio4" should "contar número ocurrencias por palabra" in {

    val rdd = sc.parallelize(Seq("agua", "cereza", "gorro","agua", "agua", "gorro"))

    val out = ejercicio4(rdd)

    out.collect() shouldBe Seq(("agua", 3), ("gorro", 2), ("cereza", 1))
  }

}
