package cn.just.shinelon.GraphX

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FristGraphX {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setMaster("local")
          .setAppName("FristGraphX")

    val sc=new SparkContext(conf)

    val users: RDD[(VertexId, (String, String))] = sc.parallelize(
                    Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                           (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    // Create an RDD for edges
    val relationships: RDD[Edge[String]] = sc.parallelize(
                    Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                           Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)


    // Count all the edges where src > dst
    val count=graph.edges.filter(e => e.srcId > e.dstId).count

    println("start > end counts :"+count)

    // print all users which are postdocs
    graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.foreach(println)

    //create triplet and print them
    graph.triplets.map(triplets=>{
      triplets.srcAttr._1+" is the "+triplets.attr+" of the "+triplets.dstAttr._1
    }).foreach(println)





  }

}
