package cn.just.shinelon.MLlib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 带有索引的行矩阵
  * 数据如下：
  * 1 2 3
    4 5 6
    7 8 9
  */
object TestIndexRowMatrix {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setAppName("TestIndexRowMatrix")
          .setMaster("local")
    val sc=new SparkContext(conf)

    val rdd=sc.textFile("C:\\Users\\shinelon\\Desktop\\Martirx.txt")
        .map(_.split(" ").map(_.toDouble))
        .map(line=>Vectors.dense(line))     //转换为向量存储
        .map((vd)=>new IndexedRow(vd.size,vd))    //转化格式
    val irm=new IndexedRowMatrix(rdd)     //建立索引行矩阵实例
    println(irm.getClass)                 //打印类型
    irm.rows.foreach(println)             //打印数据内容
    irm.toRowMatrix().rows.foreach(println)      //转换为行矩阵，还可以转化为坐标矩阵和块矩阵
  }

  /**
    * 打印结果如下：
    * IndexedRow(3,[1.0,2.0,3.0])
      IndexedRow(3,[4.0,5.0,6.0])
      IndexedRow(3,[7.0,8.0,9.0])
    */
}
