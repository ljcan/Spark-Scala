package cn.just.shinelon.MLlib

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors

/**
  * MLlib本地向量集的使用
  * MLlib的数据格式中，在1.3版本中仅支持整数和浮点型数
  */
object TestVector {
  def main(args: Array[String]): Unit = {
    //建立密集向量
    val vd : linalg.Vector=Vectors.dense(1,0,3)
    //打印密集向量的第三个值
    println(vd(2))
    //建立稀疏向量
    //sparse()函数有三个参数，第一个是size，即数据的大小，第二个参数为数据向量的下标，这里从1开始，第三个参数是输入的数据
    //注意，第二个参数严格按照增序的顺序增加数据的下标，范围为第二个参数的第一个数到第二个参数的大小（前开后闭，例如下面是[0,4)）
    val vs:linalg.Vector = Vectors.sparse(4,Array(0,1,2,3),Array(5,9,7,3))
    println(vs(2))
  }
}
