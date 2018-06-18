package main.scala.cn.shinelon.com

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 本地运行无法创建broadcast，因此会报空指针异常
  */
object SparkSqlDemo02 {

  case class Person(name: String,age: Int)

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setAppName("SparkSqlDemo02")
          .setMaster("local")



    val sc=new SparkContext(conf)

    val sqlContext=new SQLContext(sc)


    //隐式转换为一个DataFrame
    import sqlContext.implicits._

    //使用parquetFile读取集群上hdfs文件文件系统上文件需要把core-site.xml和hdfs-site.xml文件放置在项目的resources目录下。
    val parquetFile=sqlContext.parquetFile("hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/data/test/key=1/part-r-00001.parquet")
    //将DataFrame注册为临时表，提供SQL查询使用
    parquetFile.registerTempTable("parquetTaable")

    val result=sqlContext.sql("select * from parquetTaable")

    result.map(t=>"Name:"+t(0)).collect().foreach(println)

    /**
      * 在Spark-shell中运行结果如下：
      * Name:1
        Name:2
      */
  }

}
