package cn.just.shinelon.MLlib.Algorithm

import org.apache.spark.{SparkConf, SparkContext}

object TF_IDF02 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local")
      .setAppName("TF_IDF")

    val sc=new SparkContext(conf)

    val text=sc.textFile("F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\MLlib\\project\\data\\tf_idf.txt")

    //总单词数
    val totalWordCount=text.flatMap(_.split(" ")).count()

    println(totalWordCount)

    //计算文章的总词数
    val textWordCount=text.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)

    //计算IDF
    val idf=math.log(totalWordCount.toDouble/(1+1).toDouble)

    textWordCount.foreach(x=>{
      val word_tf=x._2.toDouble/totalWordCount.toDouble
      val tf_idf=word_tf*idf
      println(x._1,tf_idf)
    })


  }

}
