package exercise2

import exercise2.Dummy.generarTuplas
import org.apache.spark.rdd.RDD
import utils.TestInit

class DummyTest extends TestInit{

  val sc = spark.sparkContext

  "Rdd" should "generarTublas" in {
    val rdd: RDD[String] = sc.parallelize(Seq("a","b","c"))
    val out = generarTuplas(rdd)
    println(out.collect().mkString("\n"))
  }


}
