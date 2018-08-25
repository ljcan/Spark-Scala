package cn.just.shinelon.GraphX

import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算各个顶点的三角形数目，要求边的指向(srcId < dstId)
  */
object TestTriangleCount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local[4]")
      .setAppName("TestTriangleCount")

    val sc=new SparkContext(conf)

    //加载边和分区图
    val graph = GraphLoader.edgeListFile(sc,"F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\txt\\followers.txt",true)
              .partitionBy(PartitionStrategy.RandomVertexCut)

    //找出每个顶点的三角形数
    val triCounts = graph.triangleCount().vertices

    //加入用户名的三角形数
    val users = sc.textFile("F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\txt\\users.txt").map(lines=>{
      val fields=lines.split(",")
      (fields(0).toLong,fields(1))
    })

    val triCountByUsername=users.join(triCounts).map({
      case (id,(username,tc))=>(username,tc)
    })

    println(triCountByUsername.collect().mkString("\n"))
  }

}
