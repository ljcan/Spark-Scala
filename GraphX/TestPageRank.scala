package cn.just.shinelon.GraphX

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

object TestPageRank {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setMaster("local[4]")
          .setAppName("TestPageRank")

    val sc=new SparkContext(conf)

    //加载图的边 edge
    val graph=GraphLoader.edgeListFile(sc,"F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\txt\\followers.txt")

    //运行PageRank
    val ranks=graph.pageRank(0.0001).vertices

    ranks.foreach(println)

    //使用用户名join rank
    //user vertext
    val users=sc.textFile("F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\txt\\users.txt").map(line=>{
      val fields=line.split(",")
      (fields(0).toLong,fields(1))
    })

    val ranksByUsername=users.join(ranks).map({
      case (id,(username,rank))=>(username,rank)
    })

    //打印结果
    println(ranksByUsername.collect().mkString("\n"))

  }

}
