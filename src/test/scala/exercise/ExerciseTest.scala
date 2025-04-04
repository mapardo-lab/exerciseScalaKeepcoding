package exercise

import exercise.Exercise.{codificar, compressRecursive, invertir, isPalindrome, longitud, nUltimo, penultima, ultimo}
import org.scalatest.FlatSpec
import org.scalatest.Matchers.convertToAnyShouldWrapper

class ExerciseTest extends FlatSpec{

  "Test función ultimo" should "devolver último elemento de lista de cualquier tipo" in {
    val listInt: List[Int] = List(1,2,3,4,5)
    ultimo(listInt) shouldBe 5
    ultimo(List("a","b","c")) shouldBe "c"
  }

  "Test función penultima" should "devolver penúltimo elemento de lista de cualquier tipo" in {
    val listInt: List[Int] = List(1,2,3,4,5)
    penultima(listInt) shouldBe 4
    penultima(List("a","b","c")) shouldBe "b"
  }

  "Test función nUltimo" should "devolver el elemento en posición n de lista de cualquier tipo" in {
    val listInt: List[Int] = List(1,2,3,4,5)
    nUltimo(2, listInt) shouldBe 3
    assertThrows[NoSuchElementException] {
      nUltimo(-1, listInt)
    }
    nUltimo(1, List("a","b","c")) shouldBe "b"
  }

  "Test función longitud" should "devolver la longitud de una lista de cualquier tipo" in {
    val listInt: List[Int] = List(1,2,3,4,5)
    longitud(listInt) shouldBe 5
    longitud(List("a","b","c")) shouldBe 3
  }

  "Test función invertir" should "devolver una lista de cualquier tipo con sus elementos en orden inverso" in {
    val listInt: List[Int] = List(1,2,3,4,5)
    invertir(listInt) shouldBe List(5,4,3,2,1)
    invertir(List("a","b","c")) shouldBe List("c","b","a")
  }

  "Test función isPalindrome" should "predicado que comprueba si una lista de cualquier tipo es palindromo" in {
    val listInt: List[Int] = List(1,2,3,4,5)
    isPalindrome(listInt) shouldBe false
    isPalindrome(List("a","b","c","b","a")) shouldBe true
  }

  "compressRecursive" should "work with all elements duplicated" in {
    val listChar: List[Symbol] = List('a, 'a, 'a, 'a, 'b, 'b, 'c, 'c, 'a, 'a, 'd, 'd, 'e, 'e, 'e, 'e)
    compressRecursive(listChar) shouldBe List('a, 'b, 'c, 'a, 'd, 'e)
  }
  "compressRecursive" should "work with all elements not duplicated" in {
    val listChar: List[Symbol] = List('a, 'b, 'c, 'a, 'd, 'e)
    compressRecursive(listChar) shouldBe List('a, 'b, 'c, 'a, 'd, 'e)
  }
  "compressRecursive" should "work with only one element not duplicated" in {
    val listChar: List[Symbol] = List('a)
    compressRecursive(listChar) shouldBe List('a)
  }
  "compressRecursive" should "work with only one element duplicated" in {
    val listChar: List[Symbol] = List('a, 'a, 'a, 'a)
    compressRecursive(listChar) shouldBe List('a)
  }
  "compressRecursive" should "work empty list" in {
    val listChar: List[Symbol] = List()
    compressRecursive(listChar) shouldBe List()
  }

  "codificar" should "work with all elements duplicated" in {
    val listChar: List[Symbol] = List('a, 'a, 'a, 'a, 'b, 'b, 'c, 'c, 'a, 'a, 'd, 'd, 'e, 'e, 'e, 'e)
    codificar(listChar) shouldBe List((4,'a), (2,'b), (2,'c), (2,'a), (2,'d), (4,'e))
  }
  "codificar" should "work with all elements not duplicated" in {
    val listChar: List[Symbol] = List('a, 'b, 'c, 'a, 'd, 'e)
    codificar(listChar) shouldBe List((1,'a), (1,'b), (1,'c), (1,'a), (1,'d), (1,'e))
  }
  "codificar" should "work with only one element not duplicated" in {
    val listChar: List[Symbol] = List('a)
    codificar(listChar) shouldBe List((1,'a))
  }
  "codificar" should "work with only one element duplicated" in {
    val listChar: List[Symbol] = List('a, 'a, 'a, 'a)
    codificar(listChar) shouldBe List((4,'a))
  }
  "codificar" should "work empty list" in {
    val listChar: List[Symbol] = List()
    codificar(listChar) shouldBe List()
  }

}
