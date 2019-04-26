package cn.pengzhaopeng.spark.SparkRDD.Comparable

/**
  * Sorting
  * 这个排序的接口只是对java中的Arrays.sort的简单封装，
  * 该接口源码中比较重要就是用递归的方式实现了一次快排
  */
object SortingTest {
  def main(args: Array[String]): Unit = {
    var paris = Array(("a", 5, 2), ("c", 3, 1), ("a", 1, 3))

    import scala.util.Sorting
    //单一字段从原类型到目标类型
    Sorting.quickSort(paris)(Ordering.by(_._2))
    paris.foreach(println)
    //多个字段，只需要指定目标类型
    Sorting.quickSort(paris)(Ordering[(String, Int)].on(x => (x._1, x._2)))
    //    paris.foreach(println)

    case class Person(name: String, age: Int)
    val people = Array(Person("bob", 30), Person("ann", 32), Person("carl", 19))
    //sort by age
    object AgeOrdering extends Ordering[Person] {
      override def compare(x: Person, y: Person): Int = x.age compare y.age
    }
    Sorting.quickSort(people)(AgeOrdering)
    //    people.foreach(println)

    val p1 = new Person("aain", 24)
    val p2 = new Person("cain", 22)
    val p3 = new Person("bily", 15)
    val list = List(p1, p2, p3)
    implicit object PersonOrdering extends Ordering[Person] {
      override def compare(x: Person, y: Person): Int = {
        x.name == y.name match {
          case false => x.name.compareTo(y.name)
          case _ => x.age compare y.age
        }
      }
    }
    val list1: List[Person] = list.sorted
    list1.foreach(println)
  }
}
