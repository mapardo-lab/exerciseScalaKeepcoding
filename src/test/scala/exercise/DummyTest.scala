package exercise

import exercise.Dummy.prueba
import org.scalatest.FlatSpec
import org.scalatest.Matchers.convertToAnyShouldWrapper

class DummyTest extends FlatSpec{

  "Test" should "whatever" in {
    val listInt: List[Int] = List(1,2,3,4,5)
    prueba(listInt) shouldBe List(6,7,8,9,10)
  }

}
