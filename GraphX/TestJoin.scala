package cn.just.shinelon.GraphX

import org.apache.spark.graphx.{GraphLoader, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestJoin {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local[8]")
      .setAppName("TestJoin")

    val sc=new SparkContext(conf)

    val graph=GraphLoader.edgeListFile(sc,"F:\\BaiduNetdiskDownload\\web-Google.txt")

    graph.vertices.take(10).foreach(println)

    println("=====================================")

    val rawGraph=graph.mapVertices((id,attr)=>0)

    rawGraph.vertices.take(10).foreach(println)

    println("======================================")
    //计算出度
    val outDegress=rawGraph.outDegrees

    //添加一个新的属性替换原来的属性
    //使用了多参数列表的curried模式f(a)(b)= f(a,b)
    val tmp=rawGraph.joinVertices(outDegress)((_,attr,optDeg)=>optDeg)
    tmp.vertices.take(10).foreach(println)

    println("======================================")

//    val nonUniqueCosts : RDD[(VertexId,Double)]
//
//    val uniqueCosts:VertexRDD[Double]=graph.vertices.aggregateUsingIndex(nonUniqueCosts,(a,b)=>a+b)
//
//    val joinedGraph=graph.joinVertices(uniqueCosts)((id,oldCost,extraCost)=>(oldCost+extraCost).toInt)
//
//    joinedGraph.vertices.take(10).foreach(println)

    /**
      * outerJoinVertices
      */
    val degressGraph=graph.outerJoinVertices(outDegress)((id,oldAttr,outDegOpt)=>{
      outDegOpt match {
        case Some(outDegOpt) => outDegOpt
        case None => 0
      }
    })

    degressGraph.vertices.take(10).foreach(println)


  }

}
