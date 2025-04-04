package exercise

object Exercise {
  /** P01 (*) Encuentra el último elemento de una lista.
   *
   * Ejemplo:
   *
   * scala> último(Lista(1, 1, 2, 3, 5, 8))
   *
   * res0: entero = 8
   *
   * El inicio de la definición de last debe ser
   *
   * El `[A]` nos permite manejar listas de cualquier tipo.
   */
  def ultimo[A](ls: List[A]): A = ls.last


  /** P02 (*) Encuentra el penúltimo elemento de una lista.
   *
   * Por convención, el primer elemento de la lista es el elemento 0.
   *
   * Ejemplo:
   *
   * scala> penultima(List(1, 1, 2, 3, 5, 8))
   *
   * res0: entero = 5
   */
  def penultima[A](ls: List[A]): A = ls(ls.length-2)


  /** P03 (*) Encuentra el Késimo elemento de una lista.
   *
   * Por convención, el primer elemento de la lista es el elemento 0.
   *
   * Ejemplo:
   *
   * scala> nth(2, List(1, 1, 2, 3, 5, 8))
   *
   * res0: entero = 2
   */

  def nUltimo[A](n: Int, ls: List[A]): A = n match {
    case value if value <= ls.length - 1 && value >= 0 => ls(value)
    case _ => throw new NoSuchElementException
  }


  /** P04 (*) Encuentra el número de elementos de una lista.
   *
   * Ejemplo:
   *
   * scala> longitud(Lista(1, 1, 2, 3, 5, 8))
   *
   * res0: entero = 6
   */
  // Funciones incorporadas.
  def longitud[A](ls: List[A]): Int = ls.length


  /** P05 (*) Invertir una lista.
   *
   * Ejemplo:
   *
   * scala> invertir(Lista(1, 1, 2, 3, 5, 8))
   *
   * res0: Lista[Int] = Lista(8, 5, 3, 2, 1, 1)
   */

  // Incorporado.
  //def invertir[A](ls: List[A]): List[A] = ls.reverse
  //def invertir[A](ls: List[A]): List[A] = ls match {
  //  case Nil => Nil
  //  case head :: tail => invertir(tail) :+ head
  //}
  def invertir[A](ls: List[A]): List[A] = ls match {
    case Nil => Nil
    case head :: tail =>
      val result = invertir(tail) :+ head
      result
  }


  /** P06 (*) Averiguar si una lista es un palíndromo.
   *
   * Ejemplo:
   *
   * scala> esPalíndromo(Lista(1, 2, 3, 2, 1))
   *
   * res0: Booleano = verdadero
   */
  def isPalindrome[A](ls: List[A]): Boolean = invertir(ls) == ls

  /** En teoría, podríamos ser un poco más eficientes que esto. Este enfoque
   * recorre la lista dos veces: una para invertirla y otra para comprobar la igualdad.
   * Técnicamente, solo necesitamos verificar la igualdad en la primera mitad de la lista.
   * con la primera mitad de la lista invertida. El código para hacer eso más
   * de manera más eficiente que esta implementación es mucho más complicada, por lo que
   * Deja las cosas con esta implementación clara y concisa.
   */


  /** P07 (**) Eliminar duplicados consecutivos de elementos de la lista.
   *
   * Si una lista contiene elementos repetidos, deben reemplazarse con una copia única del elemento. El orden de los
   * elementos no debe ser cambió.
   *
   * Ejemplo:
   *
   * scala> compress(List('a, 'a, 'a, 'a, 'b, 'c, 'c, 'a, 'a, 'd, 'e, 'e, 'e, 'e))
   *
   * res0: List[Símbolo] = List('a, 'b, 'c, 'a, 'd, 'e)
   */

  def compressRecursive[A](ls: List[A]): List[A] = ls match {
    case Nil => Nil
    case head :: Nil => head::Nil
    case head :: body :: tail if head != body => head :: compressRecursive(body :: tail)
    case head :: body :: tail if head == body => compressRecursive(body :: tail)
    }

  /** P08 (*) Codificación de longitud de ejecución de una lista.
   *
   * Utilice el resultado del problema P09 para implementar la llamada longitud de ejecución. Metodo de compresión de
   * datos de codificación. Los duplicados consecutivos de elementos son codificado como tuplas (N, E) donde N es el
   * número de duplicados de* la elemento E.
   *
   * Ejemplo:
   *
   * scala> codificar(Lista('a, 'a, 'a, 'a, 'b, 'c, 'c, 'a, 'a, 'd, 'e, 'e, 'e, 'e))
   *
   * res0: Lista[(Int, Símbolo)] = Lista((4,'a), (1,'b), (2,'c), (2,'a), (1,'d), (4,'e))
   */

  def codificar[A](ls: List[A], num: Int=1): List[(Int, A)] = ls match {
      case Nil => Nil
      case head :: Nil => List((num, head))
      case head :: body :: tail if head != body => (num, head) :: codificar(body :: tail)
      case head :: body :: tail if head == body => codificar(body :: tail, num + 1)
    }


  /** P09 (*) Codificación de longitud de ejecución de una lista.
   *
   * Utilice el resultado del problema P09 para implementar la llamada longitud de ejecución. Metodo de compresión de
   * datos de codificación. Los duplicados consecutivos de elementos son codificado como tuplas (N, E) donde N es el
   * número de duplicados de la elemento E.
   *
   * Ejemplo:
   * scala> codificar(List('a, 'a, 'a, 'a, 'b, 'c, 'c, 'a, 'a, 'd, 'e, 'e, 'e, 'e))
   *
   * res0: Lista[(Int, Símbolo)] = List('a, 'b, 'c, 'a, 'd, 'e)
   */
  object P09 {
    def encode[A](ls: List[A]): List[(Int, A)] = ???
  }
}
