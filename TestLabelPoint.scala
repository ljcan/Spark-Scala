package cn.just.shinelon.MLlib

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * 向量标签的使用
  */
object TestLabelPoint {
  def main(args: Array[String]): Unit = {
    //建立密集向量
    val vd:linalg.Vector=Vectors.dense(2,0,6)
    //对密集向量建立标记点
    val pos=LabeledPoint(1,vd)
    //打印标记内容数据
    println(pos.features)
    //打印既定标记
    println(pos.label)
    //建立稀疏向量
    val vs:linalg.Vector = Vectors.sparse(4,Array(0,1,2,3),Array(9,5,2,7))
    //对稀疏向量建立标记点
    val neg = LabeledPoint(2,vs)
    //打印标记点内容数据
    println(neg.features)
    //打印既定标记
    println(neg.label)
  }
}
