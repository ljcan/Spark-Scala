package cn.just.shinelon.MLlib.Statistics

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 两组数据相关系数的计算
  *
  * Pearson 相关系数公式如下：
    皮尔逊相关系数：可以看出两组数据的向量夹角的余弦，用来描述两组数据的分开程度


    皮尔逊相关系数代表两组数据的余弦分开程度，表示随着数据量的增加，两组数据差别将增大。
    斯皮尔曼系数更加更注重两组数据的拟合程度，即两组数据量增加而增长曲线不变

    测试数据：
    1 2 3 4 5 6
    2 4 6 8 10 12
  *
  *
  */
object TestCorrect {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setAppName("TestCorrect")
          .setMaster("local")
    val sc=new SparkContext(conf)

    val rddX=sc.textFile("C:\\Users\\shinelon\\Desktop\\a.txt")
        .flatMap(_.split(" ")
          .map(_.toDouble))

    val rddY=sc.textFile("C:\\Users\\shinelon\\Desktop\\b.txt")
          .flatMap(_.split(" ")
            .map(_.toDouble))
    //计算不同数据之间的相关系数
    val correlation1:Double=Statistics.corr(rddX,rddY)
    //使用斯皮尔曼计算不同数据之间的相关系数
    val correlation2:Double=Statistics.corr(rddX,rddY,"spearman" )
    println(correlation1)
    println(correlation2)

    /**
      * 运行结果如下：
      * 1.0
        0.999999999999998
      */
  }
}
