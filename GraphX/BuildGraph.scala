package cn.just.shinelon.GraphX

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkConf, SparkContext}

object BuildGraph {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local[4]")
      .setAppName("FristGraphX")

    val sc=new SparkContext(conf)

//    使用顶点文件构造图，文件中只有srcId,dstId，顶点和边的属性默认为1
    val graph=GraphLoader.edgeListFile(sc,"F:\\BaiduNetdiskDownload\\web-Google.txt")

    graph.vertices.take(10).foreach(println)

//    生成随机图
    val count=GraphGenerators.logNormalGraph(sc,numVertices = 100).mapVertices((id,_)=>id.toDouble).vertices.count()

    println(count)

    //初始化一个随即图，节点的度数符合对数随机分布，边属性初始化1
    val graph2=GraphGenerators.logNormalGraph(sc,100,100).mapVertices((id,_)=>id.toDouble)
//    println(graph2.edges.count())
    graph2.vertices.take(10).foreach(println)
  }

}
