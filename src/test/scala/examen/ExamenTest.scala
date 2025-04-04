package examen

import examen.Examen._
import utils.TestInit

class ExamenTest extends TestInit{

  val sc = spark.sparkContext

  "ejercicio4" should "contar número ocurrencias por palabra" in {

    val rdd = sc.parallelize(Seq("agua", "cereza", "gorro","agua", "agua", "gorro"))

    val out = ejercicio4(rdd)

    out.collect() shouldBe Seq(("agua", 3), ("gorro", 2), ("cereza", 1))
  }

}
