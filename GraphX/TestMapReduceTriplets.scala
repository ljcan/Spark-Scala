package cn.just.shinelon.GraphX

import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用mapReduceTriplet算子计算可以到达相同目标顶点的平均年龄
  */
object TestMapReduceTriplets {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local[4]")
      .setAppName("TestMapReduceTriplets")

    val sc=new SparkContext(conf)

    //创建一个图vertex属性age
    val graph:Graph[Double,Int] = GraphGenerators.logNormalGraph(sc,100,100).mapVertices((id,_)=>id.toDouble)

    //计算older followers的人数和他们的总年龄
    val olderFollowers:VertexRDD[(Int,Double)] = graph.mapReduceTriplets[(Int,Double)](
      triplet => {      //map function
        if(triplet.srcAttr > triplet.dstAttr){
          //发送消息到目标vertex
//          println(triplet.dstId+"==="+triplet.srcAttr)
          Iterator((triplet.dstId,(1,triplet.srcAttr)))
        }else{
          //不发送消息
          Iterator.empty
        }
      },
      //增加计数和年龄
      //计算目标顶点相同的顶点的年龄
      (a,b) => (a._1+b._1,a._2+b._2)        //reduce function
      //reduce函数的作用是将合并所有以顶点作为目标节点的集合消息
    )

//    olderFollowers.count()
    //获取平均年龄
    val avgAgeOfOlderFollwer=olderFollowers.mapValues((id,value)=>value match {
      case (count,totalAge) => totalAge/count
    })

    //显示结果
    avgAgeOfOlderFollwer.collect().foreach(println)



  }

}
