package cn.just.shinelon.MLlib

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 坐标矩阵Demo
  * 数据如下：
  * 1 2 3
    4 5 6
    7 8 9

  源码如下：@Since("1.0.0")  将坐标矩阵转化为索引行矩阵
  def toIndexedRowMatrix(): IndexedRowMatrix = {
    //坐标矩阵的列数
    val nl = numCols()
    if (nl > Int.MaxValue) {
      sys.error(s"Cannot convert to a row-oriented format because the number of columns $nl is " +
        "too large.")
    }
    val n = nl.toInt
    val indexedRows = entries.map(entry => (entry.i, (entry.j.toInt, entry.value)))
      .groupByKey()
      .map { case (i, vectorEntries) =>
        IndexedRow(i, Vectors.sparse(n, vectorEntries.toSeq))
      }
    new IndexedRowMatrix(indexedRows, numRows(), n)
  }


   @Since("1.0.0")
  def sparse(size: Int, elements: Seq[(Int, Double)]): Vector = {
    require(size > 0, "The size of the requested sparse vector must be greater than 0.")

    val (indices, values) = elements.sortBy(_._1).unzip
    var prev = -1
    indices.foreach { i =>
      require(prev < i, s"Found duplicate indices: $i.")
      prev = i
    }
    require(prev < size, s"You may not write an element to index $prev because the declared " +
      s"size of your vector is $size")

    new SparseVector(size, indices.toArray, values.toArray)    //(9,[5],[6.0]) 坐标矩阵有9列
  }



   下面代码转化为索引行矩阵结果如下：
   IndexedRow(4,(9,[5],[6.0]))
···IndexedRow(1,(9,[2],[3.0]))
···IndexedRow(7,(9,[8],[9.0]))



  *
  */
object TestCoordinateRowMatrix {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setAppName("TestCoordinateRowMatrix")
          .setMaster("local")
    val sc=new SparkContext(conf)

    val rdd=sc.textFile("C:\\Users\\shinelon\\Desktop\\Martirx.txt")
          .map(_.split(" ").map(_.toDouble))
          .map(vue=>(vue(0).toLong,vue(1).toLong,vue(2)))     //转化为坐标格式
          .map(vue2=>new MatrixEntry(vue2._1,vue2._2,vue2._3))    //转化为坐标矩阵格式

    val crm=new CoordinateMatrix(rdd)     //实例化坐标矩阵

    crm.entries.foreach(println)        //打印数据
    crm.toIndexedRowMatrix().rows.foreach(println)    //转化为带有索引的行矩阵（高级矩阵可以转化为低级矩阵）

    /**
      * 打印结果如下：
      * MatrixEntry(1,2,3.0)
        MatrixEntry(4,5,6.0)
        MatrixEntry(7,8,9.0)
      */
  }
}
