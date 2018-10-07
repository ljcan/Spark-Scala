package cn.just.shinelon.MLlib.Algorithm

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.{SparkConf, SparkContext}

object TF_IDF {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setMaster("local")
          .setAppName("TF_IDF")

    val sc=new SparkContext(conf)

    val text=sc.textFile("F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\MLlib\\project\\data\\tf_idf.txt")
                .map(_.split(" ").toSeq)

    //创建TF计算实例
    val hashingTF =new HashingTF()
    //计算文档的TF值
    val tf=hashingTF.transform(text).cache()
    //创建IDF实例并且计算
    val idf=new IDF().fit(tf)

    //计算词频
    val tf_idf=idf.transform(tf)


    tf_idf.foreach(println)


  }


}
