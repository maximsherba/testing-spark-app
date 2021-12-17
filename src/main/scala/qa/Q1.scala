package qa

object Q1 extends App {


  val map = Map("name" -> "Aleksandr", "surname" -> "Nefedjev")

  List(("name", "Aleksandr"), ("surname", "Nefedjev"))

  case class Man(name: String, surname: String)

  val man: Man = map.foldLeft(Man("", "")) {
    case (acc, el) if el._1 == "name" => acc.copy(name = el._2)
    case (acc, el) if el._1 == "surname" => acc.copy(surname = el._2)
  }

  //  var i = 10
  //  var acc = 0
  //  while (i > 0) {
  //    i = i - 1
  //    acc = acc + 1
  //    println(s"$i acc= $acc")
  //  }

  private val inclusive = 0 to 10
  val ints: List[Int] =
    inclusive
      .foldLeft(List.empty[Int]) {
        case (acc, el) => acc :+ el
      }


  import scala.collection.mutable.ArrayBuffer

  val inputArr: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 3, 3, 5, 7, 8)
  var i = 0
  var dup = 0
  while (i < inputArr.length) {
    val el = inputArr(i)
    if (el == dup) inputArr -= el
    i = i + 1
    dup = el
  }


  def deduplicate(input: List[Int]): List[Int] = {

    def helper(input: List[Int], dup: Int, acc: List[Int]): List[Int] = input match {
      case Nil => acc
      case head :: tail if head == dup => helper(tail, head, acc)
      case head :: tail => helper(tail, head, acc :+ head)
    }

    helper(input, 0, List.empty[Int])
  }

  println(deduplicate(List(1, 3, 3, 5, 7, 8)))
  println(deduplicate(List(1, 3, 5, 7, 8)))
}
