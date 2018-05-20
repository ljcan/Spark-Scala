package cn.shinelon.com

import org.apache.spark.{SparkConf, SparkContext}

object FristSparkApp {
  def main(args: Array[String]) {

    val file="hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/spark/wc.input"
    val conf=new SparkConf().setAppName("Frist SparkApp").setMaster("local")
    val sc=new SparkContext(conf)
    val rdd=sc.textFile(file)
    val spRdd=rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    spRdd.saveAsTextFile("hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/spark/sparkDemoOutput")
    sc.stop()

  }

}
