package exercise2

import org.apache.spark.rdd.RDD

object Dummy {

  def generarTuplas(rdd: RDD[String]) = rdd.map(s => (s,1))


}
