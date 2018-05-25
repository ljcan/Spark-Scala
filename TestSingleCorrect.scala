package cn.just.shinelon.MLlib.Statistics

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试单个数据集相关系数的计算（一行数据不能计算）
  */
object TestSingleCorrect {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setAppName("TestSingleCorrect")
          .setMaster("local")

    val sc=new SparkContext(conf)

    val rdd=sc.textFile("C:\\Users\\shinelon\\Desktop\\a.txt")
          .map(_.split(" ").map(_.toDouble))
          .map(line=>Vectors.dense(line))     //转为向量
    println(Statistics.corr(rdd,"spearman"))    //使用斯皮尔曼计算相关系数（有点类似笛卡尔积）
  }
}
