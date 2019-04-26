package cn.pengzhaopeng.spark.SparkRDD.Comparable

object SortRules {
  implicit object OrderingXiaoRou extends Ordering[XianRou] {
    override def compare(x: XianRou, y: XianRou): Int = {
      if(x.fv == y.fv) {
        x.age - y.age
      } else {
        y.fv - x.fv
      }
    }
  }
}
