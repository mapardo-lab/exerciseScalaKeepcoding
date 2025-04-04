package examen

import org.apache.spark.rdd._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Examen {

  /**
   * Filtra los estudiantes con una calificación mayor que 8
   *
   * @param estudiantes contiene nombre, edad y calificación
   * @return
   */
    // TODO convertir el dataframe a tupla y devolverlo para luego incluirlo en el test should
  def ejercicio1a(estudiantes: DataFrame): DataFrame = estudiantes.filter(col("calificacion") > 8)

  /**
   * Devolver los nombres ordenados por orden ascendente de calificación
   *
   * @param estudiantes contine nombre, edad y calificación
   * @return
   */
  // TODO convertir el dataframe a tupla y devolverlo para luego incluirlo en el test should
  def ejercicio1b(estudiantes: DataFrame): DataFrame = estudiantes
    .sort(desc("calificacion"))
    .select("nombre")


  /**Ejercicio 2: UDF (User Defined Function)
   Pregunta: Define una función que determine si un número es par o impar.
   Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */
  def ejercicio2(numeros: DataFrame): DataFrame =  {
    =
  }




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
