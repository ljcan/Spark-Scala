package cn.just.shinelon.GraphX

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestSubGraph {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local[4]")
      .setAppName("FristGraphX")

    val sc=new SparkContext(conf)

    val users: RDD[(VertexId, (String, String))] = sc.parallelize(
         Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
               (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    // Create an RDD for edges
    val relationships: RDD[Edge[String]] = sc.parallelize(
         Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
               Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
               Edge(2L,0L,"student"),Edge(5L,0L,"colleague")))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    val graph=Graph(users,relationships,defaultUser)

    graph.triplets.map(triplet=>{
      triplet.srcAttr._1+" is the "+triplet.attr+" of "+triplet.dstAttr._1
    }).collect().foreach(println)

    println("==========build subgraph============")
    //删除多余的顶点和与之相连的边，构建一个子图
    val validGraph=graph.subgraph(vpred = (id,attr)=>attr._2!="Missing")

    validGraph.vertices.foreach(println)

    validGraph.triplets.map(triplet=>{
      triplet.srcAttr._1+" is the "+triplet.attr+" of "+triplet.dstAttr._1
    }).collect().foreach(println)

    //计算每个顶点的连接组件成员并返回一个图的顶点值
    val ccGraph=graph.connectedComponents()

//    ccGraph.vertices.foreach(println)

    //mask函数
    val validCCGraph=ccGraph.mask(validGraph)

    validCCGraph.edges.foreach(println)




  }

}
