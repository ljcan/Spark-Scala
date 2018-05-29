package main.scala.cn.shinelon.com

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark实现topn的问题
  */
object Top3 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setAppName("Top3")
          .setMaster("local")

    val sc=new SparkContext(conf)

    val rdd=sc.textFile("C:\\Users\\shinelon\\Desktop\\top3.txt")

    val mapRDD=rdd.map(num=>{
      (num.toInt,num)
    })

    val sortRDD=mapRDD.sortByKey(false)

    val result=sortRDD.map(map=>{
      map._2
    })

    val list=result.take(3)

    for(num<-list){
      println(num)
    }

    sc.stop()
  }
}
