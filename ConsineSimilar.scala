package cn.just.shinelon.MLlib.Algorithm


import org.apache.spark.metrics.source
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 余弦相似度
  */
object ConsineSimilar {

  val conf=new SparkConf()
    .setAppName("ConsineSimilar")
    .setMaster("local")

  val sc=new SparkContext(conf)

  //实例化环境
  val users=sc.parallelize(Array("aaa","bbb","ccc","ddd","eee"))
  //设置电影名
  val films=sc.parallelize(Array("smzdm","yixb","znh","nhsc","fcwr"))
  //使用一个source嵌套map作为姓名电影名和分值的存储
  var source=Map[String,Map[String,Int]]()
  //设置一个用以存放电影分的map
  val filmSource=Map[String,Int]()

  /**
    * 设置电影评分
    * @return
    */
  def getSource():Map[String,Map[String,Int]] = {

    val user1FilmSource= Map("smzdm" -> 2,"yixb" -> 3,"znh" -> 1,"nhsc" -> 0,"fcwr" -> 1)

    val user2FilmSource= Map("smzdm" -> 1,"yixb" -> 2,"znh" -> 2,"nhsc" -> 1,"fcwr" -> 4)

    val user3FilmSource= Map("smzdm" -> 2,"yixb" -> 1,"znh" -> 0,"nhsc" -> 1,"fcwr" -> 4)

    val user4FilmSource= Map("smzdm" -> 3,"yixb" -> 2,"znh" -> 0,"nhsc" -> 5,"fcwr" -> 3)

    val user5FilmSource= Map("smzdm" -> 5,"yixb" -> 3,"znh" -> 1,"nhsc" -> 1,"fcwr" -> 2)

    source += ("aaa" -> user1FilmSource)    //对人名进行存储
    source += ("bbb" -> user2FilmSource)    //对人名进行存储
    source += ("ccc" -> user3FilmSource)    //对人名进行存储
    source += ("ddd" -> user4FilmSource)    //对人名进行存储
    source += ("eee" -> user5FilmSource)    //对人名进行存储

    source    //返回map
  }

  /**
    * 计算余弦相似性
    * @param user1
    * @param user2
    * @return
    */
  def getCollaborateSource(user1:String,user2:String): Double ={
    //获得第一个用户的评分
    val user1FilmSource = source.get(user1).get.values.toVector
    //获得第二个用户的评分
    val user2FileSource = source.get(user2).get.values.toVector
    //对欧几里得公式分子部分进行计算
    val member = user1FilmSource.zip(user2FileSource).map(num=>num._1*num._2).reduce(_+_).toDouble
    //求出分母第一个变量的值
    val temp1 = math.sqrt(user1FilmSource.map(num => {
      math.pow(num, 2)
    }).reduce(_+_)).toDouble
    //求出分母第二个变量的值
    val temp2 = math.sqrt(user2FileSource.map(num => {
      math.pow(num,2)
    }).reduce(_+_)).toDouble
    //求出分母
    val denominator = temp1*temp2
    //返回结果
    member/denominator

  }

  def main(args: Array[String]): Unit = {
    //初始化分数
    getSource()
    //设定目标对象
    val name="bbb"
    //迭代进行计算
    users.foreach(user=>{
      println(name + " 相对于 "+user +"的相似性分数为: "+getCollaborateSource(name,user))
    })
    val frist=users.sortBy((user=>getCollaborateSource(name,user)),false,1).first()
    println("-----------------------------------------------------------")
    println("相似度最高的用户为："+frist)

    /**
      * 计算结果如下：
      * bbb 相对于 aaa的相似性分数为: 0.7089175569585667
        bbb 相对于 bbb的相似性分数为: 1.0000000000000002
        bbb 相对于 ccc的相似性分数为: 0.8780541105074453
        bbb 相对于 ddd的相似性分数为: 0.6865554812287477
        bbb 相对于 eee的相似性分数为: 0.6821910402406466
      */

  }
}
