package cn.just.shinelon.MLlib.Statistics

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs._

/**
  * 随机数
  * 使用RandomRDDs随机类生成随机数
  */
object TestRandomRDD {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setAppName("TestRandomRDD")
      .setMaster("local")

    val sc=new SparkContext(conf)

    val random=normalRDD(sc,10)    //静态调用类，随机生成10个随机数
    random.foreach(println)
  }

}
