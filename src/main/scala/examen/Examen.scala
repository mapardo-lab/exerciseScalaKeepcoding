package examen

import org.apache.spark.rdd._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Examen {

  object Ejercicio1 {
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
  }

  object Ejercicio2 {

    def esNegativo(num: Double): Boolean = num < 0

    val udfEsNegativo = udf((num: Double) => esNegativo(num))

    /**
     * Aplica función UDF al dataframe dfTemperaturas
     *
     * @param dfTemperaturas contiene nombre ciudad y temperatura mínima
     * @return contiene nombre ciudad, temperatura mínima y el valor de temperatura es negativa
     */
    def ejercicio2(dfTemperaturas: DataFrame): DataFrame = dfTemperaturas.
      withColumn("bajoCero", udfEsNegativo(col("temperaturaMinima")))
  }

  object Ejercicio3 {
    /**
     * Une dataframes y calcula la nota media de cada alumno
     *
     * @param estudiantes contiene id y nombre de alumno
     * @param calificaciones contiene id de alumno, asignatura y calificación
     * @return
     */
    def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id"))
      .groupBy("nombre")
      .agg(round(mean("resultado"),1).alias("media"))

  }
    object Ejercicio4 {
      /**
       * Cuenta el número de ocurrencias por palabra
       *
       * @param rdd Conjuntos de palabras
       * @return Tupla con la palabra y el número de veces que aparece
       */
      def ejercicio4(rdd: RDD[String]): RDD[(String, Int)] = rdd
        .map((_, 1))
        .groupByKey()
        .map { case (word, values) => (word, values.sum) }
        .sortBy(-_._2)
    }
}