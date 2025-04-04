package exercise2

import exercise2.spUtils.SparkUtils.runSparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.{Calendar, Locale}

object Exercise2 {
  /**
   * Crear un RDD apartir del fichero (README.md ) ejecutaracciones/transformaciones paracontestar:
   *
   * • ¿Cuántas líneas tiene el fichero?
   * • ¿En cuántas línea saparece la palabra “spark”?
   * • Imprimir por pantalla el num de palabras de 5 líneas
   */
  def ejercicio2(fileRDD: RDD[String])(implicit spark: SparkSession): Unit = {

    println(s"El fichero README.md contiene ${fileRDD.count} líneas")
    println(s"El fichero README.md contiene ${fileRDD.filter(line => line.contains("spark")).count} lineas con la palabra spark")
    val linea = fileRDD.take(30).lastOption
    val numWords = linea.getOrElse("").split("\\s+").length
    println(s"Número de palabras en la linea 5: ${numWords}")
  }
}

/**
 * Definir función plural
 * <ul>
 * <li> Pasar wordsRDD a plural.
 * <li> Calcular la longitud de cada palabra
 * <li> Trasformar los elementos en pares de elementos (palabra, 1) Contar ocurrencias de palabras
 * <ul>
 *    <li> Con ReduceByKey
 *    <li> Con GroupByKey
 * </ul>
 * </ul>
 */
object Exercise3 {
  val vowels = "aeiuo"
  // Pasar wordsRDD a plural
    def ejercicio3wordsToPlural(wordsRDD: RDD[String]): RDD[String] = wordsRDD.map {
        case word if vowels.contains(word.last) => word :+ 's'
        //case word if isVowel(word.last) => word :+ 's'
        case word if 's' == word.last => word
        case word => word + "es"
      }

    def isVowel(char: Char): Boolean = "aeiuo".contains(char)

  // Calcular la longitud de cada palabra
    def ejercicio3wordsLong(rddPlural: RDD[String]): RDD[Int] = rddPlural.map(_.length)

  // Trasformar los elementos en pares de elementos (palabra, 1) Contar ocurrencias de palabras
    def ejercicio3tubla(rddPlural: RDD[String]): RDD[(String,Int)] = rddPlural.map((_, 1))

  // Contar ocurrencias de palabras reduceByKey
    def ejercicio3Num(rddKv: RDD[(String,Int)]): RDD[(String,Int)] = rddKv.reduceByKey(_ + _)


  // Contar ocurrencias de palabras groupByKey
    def ejercicio3NumGroupByKey(rddKv: RDD[(String,Int)]): RDD[(String,Int)] = rddKv
        .groupByKey()
        .map {case (word, values) => (word,values.sum)}

}


/**
 * Parsear el Apache Log Server de la NASA
 * 1. Definir función para interpretar fecha
 * 2. Definir función para parsear líneas
 * 3. Definir función para parsear fichero
 * Usando el RDD access_logs calcular:
 * Mínimo,Máximo y Media del tamaño de las peticiones(size) o Nopeticionesdecadacódigoderespuesta(response_code) o Mostrar20hostsquehansidovisitadosmásde10veces
 * Mostrarlos10endpointsmásvisitados
 * Mostrarlos10endpointsmásvisitadosquenotienencódigoderespuesta=200 o Calcularelnodehostsdistintos
 * Contarelnodehostsúnicoscadadía
 * Calcularlamediadepeticionesdiariasporhost
 * Mostrarunalistade40endpointsdistintosquegenerancódigoderespuesta=404 o Mostrareltop25deendopointsquemáscódigosderespuesta404generan
 * Eltop5dedíasquesegeneraroncódigoderespuestas404
 */
case class LogRecord( host: String,
                      client_identd: String,
                      user_id: String,
                      date_time: Calendar,
                      method:String,
                      endpoint:String,
                      protocol:String,
                      response_code:Int,
                      content_size:Long)


object Exercise4 {

  def parseApacheTime(apache_time: String) : Calendar = {

    val date = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH).parse(apache_time)

    val cal = Calendar.getInstance()
    cal.setTime(date)

    cal
  }

  def parseApacheLogLine(logLine: String) : (LogRecord, Int) = {

    val logEntryPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+)\\s*(\\S*) ?\" (\\d{3}) (\\S+)"
    val pattern = Pattern.compile(logEntryPattern)
    val matcher = pattern.matcher(logLine)

    if (matcher.find())
      (LogRecord(
        matcher.group(1), // host
        matcher.group(2), // client_identd
        matcher.group(3), // user_id
        parseApacheTime(matcher.group(4)), // date_time
        matcher.group(5), // method
        matcher.group(6), // endpoint
        matcher.group(7), // protocol
        matcher.group(8).toInt, // response_code
        if (matcher.group(9) == "-") 0L else matcher.group(9).toLong) // content_size
        , 1)
    else
      (LogRecord(logLine, "", "", null, "", "", "", 0 ,0), 0)
  }

  def parseLogs(parsedLogsRdd: RDD[String])(implicit spark: SparkSession): RDD[LogRecord] = {
    import spark.implicits._
    val tupleLog = parsedLogsRdd.map{line => parseApacheLogLine(line)}
    tupleLog.map{log => log._1}
  }

  def runExercise4(logRecords: RDD[LogRecord]): Unit = {
    // Mínimo,Máximo y Media del tamaño de las peticiones(size)
    val contentSize = logRecords.map(x => x.content_size)
    println(s"Content size:")
    println(s"Min value for content size: ${contentSize.min()}")
    println(s"Max value for content size: ${contentSize.max()}")
    println(s"Max value for content size: ${contentSize.mean()}")

    // Num peticionesdecadacódigoderespuesta(response_code)
    val responseCode = logRecords.map(resp => (resp.response_code, 1))
      .reduceByKey(_ + _)
    println(s"Response code:")
    responseCode.collect().foreach {
      case (response, num) => println(s"Response code ${response} appears ${num}")
    }
    // Mostrar20hostsquehansidovisitadosmásde10veces
    val host = logRecords.map(resp => (resp.host, 1))
      .reduceByKey(_ + _)
    val selectHost = host.filter { case (_, num) => num > 10 }.top(20)
    println(s"Host:")
    selectHost.foreach {
      case (host, num) => println(s"Host ${host} appears ${num}")
    }
    // Mostrarlos10endpointsmásvisitados
    // Mostrarlos10endpointsmásvisitadosquenotienencódigoderespuesta=200 o Calcularelnodehostsdistintos
    // Contarelnodehostsúnicoscadadía
    // Calcularlamediadepeticionesdiariasporhost
    // Mostrarunalistade40endpointsdistintosquegenerancódigoderespuesta=404
    // Mostrareltop25deendopointsquemáscódigosderespuesta404generan
    // Eltop5dedíasquesegeneraroncódigoderespuestas404
  }
}

/**
 * idPartido::temporada::jornada::EquipoLocal::EquipoVisitante::golesLocal::golesVisitante::fecha::timestamp
 *
 * 4808::1977-78::8::Rayo Vallecano::Real Madrid::3::2::30/10/1977::247014000.0
 *
 * Procesar el DataSetPartidos.txt
 * <ul>
 * <li> Calcular los goles que ha marcado el Real Sporting de Gijón
 * <ul><li> Usar un acumulador para, a la vez, contar el no de partidos que terminaron 0-0</ul>
 * <li> ¿En qué temporada se marcaron más goles?
 * <li> ¿Cuál es el equipo que tiene el record de meter más goles como local? ¿Y como visitante?
 * <li> ¿Cuál son las 3 décadas en las que más goles se metieron? o ¿Qué equipo es el mejor local en los últimos 5 años?
 * <li> ¿Cuál es la media de victorias por temporada en los equipos que han estado menos de 10 temporadas en 1a división?
 * </ul>
 */
object Ejercicio5 {

  case class Partido (idPartido: String,
                      temporada: String,
                      jornada:   String,
                      equipoL:   String,
                      equipoV:   String,
                      golesL:    Int,
                      golesV:    Int,
                      fecha:     String,
                      timestamp: String)

  val SPORTING = "Sporting de Gijon"


}


/**
 * Ejercicio 6
 *
 * idPartido::temporada::jornada::EquipoLocal::EquipoVisitante::golesLocal::golesVisitante::fecha::timestamp
 *
 * 4808::1977-78::8::Rayo Vallecano::Real Madrid::3::2::30/10/1977::247014000.0
 *
 * Registrar DataSetPartidos.txt como Tabla usando y usando SQL / Core
 * <ul>
 *   <li> ¿Quién ha estado más temporadas en 1a División: Sporting u Oviedo?
 *   <li> ¿Cuál es el record de goles como visitante en una temporada del Oviedo?
 *   <li> ¿En qué temporada se marcaron más goles en Asturias?
 *    <ul>
 *      <li> GolesmarcadosyrecibidosporelSportingjugandodelocal
 *      <li> GolesmarcadosyrecibidosporelOviedojugandodelocal
 *    </ul>
 * </ul>
 */
object Ejercicio6 {

  case class Partido(idPartido: String,
                     temporada: String,
                     jornada:   String,
                     equipoL:   String,
                     equipoV:   String,
                     golesL:    Int,
                     golesV:    Int,
                     fecha:     String,
                     timestamp: String)

  def E6main(lines: Dataset[String])(implicit spark: SparkSession) = ???
}

/**
 * Ejercicio 7
 * <ul>
 * <li> Crear un DataFrame a partir del fichero natality.csv
 * <li> Utilizando el API DataFame
 * <li> Probar operaciones básicas
 * <li> Obtener en que 10 estados nacieron más niños en 2003
 * <li> Obtener la media de peso de los niños por año y estado
 * <li> Evolución por año y por mes de número de niños y niñas nacidas
 * <li> Utilizando SQL
 * <li> Responder a las misma preguntas del anterior apartado
 * <li> Crear un DataSet a partir del DataFrame
 * <li> Obtener los 3 meses de 2005 en que nacieron más niños
 * </ul>
 */
object Ejercicio7 {

  implicit val spark: SparkSession = runSparkSession("Keepkoding")

  def lecturaCsvDf(path: String, delimiter: String = ",")(implicit spark: SparkSession): DataFrame = {
    spark.read
      .options(Map(("header", "true"), ("delimiter", delimiter)))
      .csv(path)
  }

  def e7main(dfNatality: DataFrame)(implicit spark: SparkSession): Unit = {
    // Obtener en que 10 estados nacieron más niños en 2003
    dfNatality.groupBy("state").agg(count("*").alias("Total")).show()
  }

}