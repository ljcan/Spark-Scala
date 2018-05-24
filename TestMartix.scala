package cn.just.shinelon.MLlib

import org.apache.spark.mllib.linalg.Matrices

/**
  * 本地矩阵的使用
  */
object TestMartix {
  def main(args: Array[String]): Unit = {
    //创建一个 分布式矩阵
    //dense函数三个参数，第一个行数，第二个列数，第三个数据
    val mx = Matrices.dense(2,3,Array(1,2,3,4,5,6))
    println(mx)

    /**
      * 结果如下：
      * 1.0  3.0  5.0
        2.0  4.0  6.0
      */
  }
}
