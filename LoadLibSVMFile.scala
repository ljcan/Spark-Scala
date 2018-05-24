package cn.just.shinelon.MLlib

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 源码如下：
  *   private[spark] def parseLibSVMFile(
      sc: SparkContext,
      path: String,
      minPartitions: Int): RDD[(Double, Array[Int], Array[Double])] = {
    sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map(parseLibSVMRecord)
  }

  private[spark] def parseLibSVMRecord(line: String): (Double, Array[Int], Array[Double]) = {
    val items = line.split(' ')
    val label = items.head.toDouble
    val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
      val indexAndValue = item.split(':')
      val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
      val value = indexAndValue(1).toDouble
      (index, value)
    }.unzip
  *
  * 从固定的数据集中获取数据建立向量集
  *
  * 外部数据：数据格式（标签 下标1：value1 下标2：value2 ...中间以空格隔开）
  * 1 1:2 2:3 3:3 4:1
    2 1:5 2:8 3:9 4:7
    1 1:6 2:7 3:0 4:9
    3 1:5 2:1 3:2 4:5
  */
object LoadLibSVMFile {
  def main(args: Array[String]): Unit = {
      val conf=new SparkConf()
            .setAppName("LoadLibSVMFile")
            .setMaster("local")
      val sc=new SparkContext(conf)
      //读取外部文件
      val mu=MLUtils.loadLibSVMFile(sc,"C:\\Users\\shinelon\\Desktop\\MLlib.txt")
      //打印内容
      mu.foreach(println)

  }
}
