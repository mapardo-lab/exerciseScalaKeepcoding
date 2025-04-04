package examen

import org.apache.spark.rdd._

object Examen {

  /**
   * Cuenta el número de ocurrencias por palabra
   * 
   * @param rdd Conjuntos de palabras
   * @return Tupla con la palabra y el número de veces que aparece
   */
  def ejercicio4(rdd: RDD[String]): RDD[(String,Int)] = rdd
    .map((_, 1))
    .groupByKey()
    .map {case (word, values) => (word,values.sum)}
    .sortBy(-_._2)

}
