package exercise

object Dummy {

  def prueba(ls: List[Int]): List[Int] = {
    val ls1: List[Int] = ls.map(x => x + 5)
    val ls2: List[Int] = ls1.filter(x => x > 3)
    ls2
  }

}
