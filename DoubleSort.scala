package main.scala.cn.shinelon.com

import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark实现二次排序
  *
  * 原始测试数据为：
  *   1 5
      2 4
      1 3
      1 2
      5 4
      3 4
      3 5
      2 1
      2 5
      4 1
  *
  * 排序结果为：
  * 5 4
    4 1
    3 4
    3 5
    2 4
    2 1
    2 5
    1 5
    1 3
    1 2
  */
object DoubleSort {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setAppName("DoubleSort")
          .setMaster("local")

    val sc=new SparkContext(conf)

    val rdd=sc.textFile("C:\\Users\\shinelon\\Desktop\\doubleSort.txt");
    //转换数据格式，比如之前为(1 2)转换为Tuple2(DoubleSortKey(1,2),"1,2"))的格式，前面的第一个参数为自定义的排序key
    val mapRDD=rdd.map(line=>{
      (new DoubleSortKey(line(0).toInt,line(1).toInt),line)
    })
    //然后按照key排序
    //由大到小排序
    val sortRDD=mapRDD.sortByKey(false)
    //去除掉自定义的key，然后进行只输出排序好的原始结果
    val result=sortRDD.map(map=>{
      map._2
    })

    result.foreach(println)

    sc.stop()
  }
}
