package cn.just.shinelon.GraphX

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 连通分量算法标出了图中每个连通分量编号最低的顶点所连的子集
  * 即每个结果是每个顶点连通分量中的最小顶点
  */
object TestConnectedComponents {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local[4]")
      .setAppName("TestConnectedComponents")

    val sc=new SparkContext(conf)

    //加载关系图
    val graph=GraphLoader.edgeListFile(sc,"F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\txt\\followers.txt")

    //找到连接组件
    val cc=graph.connectedComponents().vertices

    //加入用户名的连接组件
    val users=sc.textFile("F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\txt\\users.txt").map(line=>{
      val fields=line.split(",")
      (fields(0).toLong,fields(1))
    })

    val ccByUsername=users.join(cc).map({
      case (id ,(username,cc))=>(username,cc)
    })

    println(ccByUsername.collect().mkString("\n"))


  }


}
