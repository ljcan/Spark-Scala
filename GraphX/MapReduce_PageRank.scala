package cn.just.shinelon.GraphX

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用mapreduce模型迭代计算pageRank
  数据格式如下：
  A B
  B A
  C A D E
  D A C
  E A
  G A C
  H A C
  I A C
  J C
  K C
  */
object MapReduce_PageRank {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setMaster("local[8]")
          .setAppName("MapReduce_PageRank")

    val sc=new SparkContext(conf)

    val lines=sc.textFile("F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\txt\\test_network.txt")

    //(C,A),(C,D),(C,E)=>(C,(A,D,E))
    val links=lines.flatMap(line=>{
      val list=line.split(" ")
      list.drop(1).map((list(0),_))
    }).groupByKey().cache()

    //初始化PR值为1.0
    //(C,(A,D,E))=>(C,(A,D,E),1.0)
    var ranks=links.mapValues(v=>1.0)



    ///迭代100次MapReduce，计算PageRank值
    for(i <- 1 to 100){
      /**
        * map操作
        */
      val contribs=links.join(ranks).values.flatMap({
        case(urls,rank)=>{
          val size=urls.size
          urls.map(url=>(url,rank/size))
        }
      }).union(links.mapValues(v=>0.0))

      /**
        * reduce操作
        */
      ranks=contribs.reduceByKey(_+_).mapValues(0.15+0.85*_)
    }


//    ranks.foreach(println)

    //输出结果
    val output=ranks.sortByKey().collect()
    output.foreach(tup=>{
      println(tup._1+" has rank: "+tup._2+".")
    })


  }

}
