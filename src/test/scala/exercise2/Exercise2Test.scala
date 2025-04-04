package exercise2

import exercise2.Ejercicio7._
import exercise2.Exercise2._
import exercise2.Exercise3._
import exercise2.Exercise4._
import utils.TestInit

class Exercise2Test extends TestInit {

  val sc = spark.sparkContext

  "ejercicio2" should "calculate" in {

    val textRDD = sc.textFile("/home/mapardo/IdeaProjects/bigdataprocess/src/test/resources/README.md")
    ejercicio2(textRDD)

  }

  "ejercicio3wordsToPrular" should "añadir plural" in {

    val rdd = sc.parallelize(Seq("pato", "oso", "caracol"))
    val out = ejercicio3wordsToPlural(rdd)

    out.collect() shouldBe Seq("patos", "osos", "caracoles")
  }

  "ejercicio3wordsLong" should "longitud" in {

    val rdd = sc.parallelize(Seq("pato", "oso", "caracol"))
    val out = ejercicio3wordsLong(rdd)

    out.collect() shouldBe Seq(4, 3, 7)
  }
  "ejercicio3tubla" should "crear tupla con info" in {

    val rdd = sc.parallelize(Seq("pato", "oso", "caracol", "pato", "pato", "caracol"))
    val out = ejercicio3tubla(rdd)

    out.collect() shouldBe Seq(("pato",1), ("oso",1), ("caracol",1), ("pato",1), ("pato",1), ("caracol",1))
  }
  "ejercicio3Num" should "cuenta palabras" in {

    val rdd = sc.parallelize(Seq("pato", "oso", "caracol", "pato", "pato", "caracol"))
    val out = ejercicio3NumGroupByKey(ejercicio3tubla(rdd))

    out.collect() shouldBe Seq(("pato",3), ("caracol",2), ("oso",1))
  }

  "parseLogs" should "exercise 4" in {

    val textRDD = sc.textFile("src/test/resources/apache.access.log")
    val logs = parseLogs(textRDD)
    runExercise4(logs)
  }

  "natality" should "exercise 7" in {
    val natalityCsv = lecturaCsvDf("src/test/resources/natality.csv")
    e7main(natalityCsv)
  }

}
