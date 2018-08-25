package cn.just.shinelon.GraphX

import org.apache.spark.graphx.{VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestAggregateUsingIndex {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local[4]")
      .setAppName("FristGraphX")

    val sc=new SparkContext(conf)

    val setA:VertexRDD[Int] = VertexRDD(sc.parallelize(0L until 100L).map(id=>(id,1)))

    val rddB:RDD[(VertexId,Double)]=sc.parallelize(0L until 100L).flatMap(id=>List((id,1.0),(id,2.0)))

//    rddB.foreach(println)

    //构建一个setA相对于rddB顶点的超集
    val setB:VertexRDD[Double]=setA.aggregateUsingIndex(rddB,_+_)

//    setB.foreach(println)

    val setC:VertexRDD[Double] =setA.innerJoin(setB)((id,a,b)=>a+b)

    setC.foreach(println)




  }

}
