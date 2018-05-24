package cn.just.shinelon.MLlib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}

/**
  * MLlib提供了四种分布式矩阵存储形式，均由支持长整形的行列数和双精度浮点型的数据内容构成
  * 四种矩阵分别为：
  * 行矩阵 带有行索引的行矩阵 坐标矩阵  块矩阵
  *
  * 分布式矩阵的使用：行矩阵的使用
  * 数据如下：
  * 1 2 3
    4 5 6
    7 8 9
  *
  */
object TestRowMatrix {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setAppName("TestRowMatrix")
          .setMaster("local")
    val sc=new SparkContext(conf)

    val rdd=sc.textFile("C:\\Users\\shinelon\\Desktop\\Martirx.txt")
          .map(_.split(" ").map(_.toDouble))   //读入行矩阵
          .map(line=>Vectors.dense(line))  //转换为Vector格式

    val rm=new RowMatrix(rdd)   //读入行矩阵
    println("行数："+rm.numRows())
    println("列数："+rm.numCols())
    println(rm.toString)    //不是最终结果，说明RowMatrix是Transformation操作
    rm.rows.foreach(println)  //打印矩阵

  }
}
