package main.scala.cn.shinelon.com

import org.apache.spark.{SparkConf, SparkContext}

object TransformationOperation {
  def main(args: Array[String]): Unit = {
    //map()
//    filter()
//    flatMap()
//    groupByKey()
//    reduceByKey()
//    sortByKey()
//    join()
    cogroup()
  }


  def map(): Unit ={
    val conf=new SparkConf()
      .setAppName("mapScala")
        .setMaster("local")
    val sc=new SparkContext(conf)

    val list=Array(1,2,3,4,5)
    //partition为1
    val numbersRDD=sc.parallelize(list,1)

    val mapRDD=numbersRDD.map(num=>num*2)

    mapRDD.foreach{num=>println(num)}

    sc.stop()
  }
  def filter():Unit={
    val conf=new SparkConf()
      .setAppName("mapScala")
      .setMaster("local")
    val sc=new SparkContext(conf)

    val list=Array(1,2,3,4,5,6,7,8,9,10)
    //partition为1
    val numbersRDD=sc.parallelize(list,1)

    val filterRDD=numbersRDD.filter(num=>(num%2==0))

    filterRDD.foreach(num=>println(num))

    sc.stop()
  }

  /**
    * flatMap算子案例
    */
  def flatMap():Unit={
    val conf=new SparkConf()
            .setAppName("flatMap")
              .setMaster("local")
    val sc=new SparkContext(conf)

    val list=Array("Hello world","Hello Spark","Be Happy")
    //并行化集合，创建初始化RDD
    val intitRDD=sc.parallelize(list,1)

    val line=intitRDD.flatMap(num=>(num.split(" ")))

    line.foreach(word=>println(word))

    sc.stop()
  }

  /**
    * groupByKey算子案例：按班级将成绩进行分组
    */
  def groupByKey():Unit={
    val conf=new SparkConf()
            .setAppName("groupByKey")
            .setMaster("local")
    val sc=new SparkContext(conf)

    val listScores=Array(Tuple2("class1",80),Tuple2("class2",85),Tuple2("class1",90),Tuple2("class2",80))

    val initRDD=sc.parallelize(listScores,1)

    val groupScores=initRDD.groupByKey()

    groupScores.foreach{
      score=>{
        println(score._1);
        score._2.foreach{
          singleScore=>println(singleScore)
        }
      }
        println("======================")
    }

    sc.stop()
  }

  /**
    * reduceByKey算子操作：计算每一个班级的总成绩
    */
  def reduceByKey():Unit={
    val conf=new SparkConf()
      .setAppName("groupByKey")
      .setMaster("local")
    val sc=new SparkContext(conf)

    val listScores=Array(Tuple2("class1",80),Tuple2("class2",85),Tuple2("class1",90),Tuple2("class2",80))

    val initRDD=sc.parallelize(listScores,1)

//    val totalScore=initRDD.reduceByKey((score1,score2)=>(score1+score2))
    val totalScore=initRDD.reduceByKey(_+_)

    totalScore.foreach(score=>println(score))

    sc.stop()
  }
  /**
    * sortByKey算子案例：按照成绩排序
   */
  def sortByKey():Unit={
    val conf=new SparkConf()
      .setAppName("groupByKey")
      .setMaster("local")
    val sc=new SparkContext(conf)

    val listScores=Array(Tuple2(85,"tom"),Tuple2(88,"lili"),Tuple2(75,"marry"),Tuple2(100,"shinelon"))

    val initRDD=sc.parallelize(listScores,1)

    val sortScore=initRDD.sortByKey(false)

    sortScore.foreach(score=>{
      print(score._1+" :"+score._2)
      println()
    })

    sc.stop()
  }

  /**
    * join算子案例：对两个RDD进行join操作
    */
  def join():Unit={
    val conf=new SparkConf()
      .setAppName("groupByKey")
      .setMaster("local")
    val sc=new SparkContext(conf)

    val students=Array(Tuple2(1,"tom"),Tuple2(2,"shinelon"),Tuple2(3,"marry"))
    val scores=Array(Tuple2(1,85),Tuple2(2,78),Tuple2(3,89))

    val studentRDD=sc.parallelize(students,1)
    val scoreRDD=sc.parallelize(scores,1)

    val studentScore=studentRDD.join(scoreRDD)

    studentScore.foreach(student=>{
      println(student._1);
      println(student._2._1);
      println(student._2._2);
      println("====================")
    })
    sc.stop()
  }

  /**
    * cogroup算子案例：对两个RDD进行cogroup操作
    */
  def cogroup():Unit={
    val conf=new SparkConf()
      .setAppName("groupByKey")
      .setMaster("local")
    val sc=new SparkContext(conf)

    val students=Array(Tuple2(1,"tom"),Tuple2(2,"shinelon"),Tuple2(3,"marry"))
    val scores=Array(Tuple2(1,85),Tuple2(2,78),Tuple2(3,89),Tuple2(2,99),Tuple2(3,85),Tuple2(1,95))

    val studentRDD=sc.parallelize(students,1)
    val scoreRDD=sc.parallelize(scores,1)

    val studentScore=studentRDD.cogroup(scoreRDD)

    studentScore.foreach(student=>{
      println(student._1);
      val x=student._2;
      val xit1=x._1.iterator
      val xit2=x._2.iterator
      while(xit1.hasNext){
        println(xit1.next())
      }
      while(xit2.hasNext){
        println(xit2.next())
      }
//      println(student._2._1);
//      println(student._2._2);
      println("====================")
    })
    sc.stop()
  }

}
