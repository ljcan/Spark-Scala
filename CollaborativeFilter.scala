package cn.just.shinelon.MLlib.Algorithm

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于ALS(交替最小二乘法)算法的协同过滤推荐
  *
  * ALS算法步骤：
  *
  * 1.切分测试集合建立数据集
  * 在MLlib中ALS算法的固定数据格式源码如下
  * @Since("0.8.0")
    case class Rating @Since("0.8.0") (
    @Since("0.8.0") user: Int,
    @Since("0.8.0") product: Int,
    @Since("0.8.0") rating: Double)
  *
  *2.训练ALS模型
  * 在MLlib中训练数据模型的方法是ALS.train()方法，其源码如下：
  *它有多个重载方法，其中一个为：
  *   @Since("0.8.1")
  def trainImplicit(
      ratings: RDD[Rating],       //需要训练的数据集
      rank: Int,                  //模型中隐藏因子数
      iterations: Int,            //算法中迭代次数
      lambda: Double,             //ALS中的正则化参数
      blocks: Int,                //并行计算的block数(-1为自动配置)
      alpha: Double,              //ALS隐式反馈变化率用于控制每次拟合修正的幅度
      seed: Long                  //加载矩阵的随机数
    ): MatrixFactorizationModel = {
    new ALS(blocks, blocks, rank, iterations, lambda, true, alpha, seed).run(ratings)
  }

测试数据如下： 用户代号 商品代号 用户对该商品的评分
            1 11 2
            1 12 3
            1 13 1
            1 14 0
            1 15 1
            2 11 1
            2 12 2
            2 13 2
            2 14 1
            2 15 4
            3 11 2
            3 12 3
            3 13 1
            3 14 0
            3 15 1
            4 11 1
            4 12 2
            4 13 2
            4 14 1
            4 15 4
            5 11 1
            5 12 2
            5 13 2
            5 14 1
            5 15 4
  *
  */
object CollaborativeFilter {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
            .setAppName("CollaborativeFilter")
            .setMaster("local")
    val sc=new SparkContext(conf)
    //设置数据集
    val data=sc.textFile("C:\\Users\\shinelon\\Desktop\\d.txt")
    //处理数据
    val ratings=data.map(_.split(" ") match {
      case Array(user,item,rate) =>                           //转化数据集
        Rating(user.toInt,item.toInt,rate.toDouble)           //将数据集转化为专用的Rating
    })
    val rank=2              //设置隐藏因子

    val numIterations=2     //设置迭代次数

    val model=ALS.train(ratings,rank,numIterations,0.01)      //进行模型训练

    val rs=model.recommendProducts(2,1)                       //为用户2推荐一个商品

    rs.foreach(println)                                       //打印推荐结果

    val result:Double=rs(0).rating                            //预测的评分结果

    val realilty=data.map(_.split(" ") match {
      case Array(user,item,rate) =>
        Rating(user.toInt,item.toInt,rate.toDouble)
    }).map(num=>{
      if(num.user==2&&num.product==15)
        num.rating                                            //返回实际评分结果
      else
        0
    }).foreach(num=>{
      if(num!=0)
        println("对15号商品预测的准确率为："+result/num)
    })
  }
}
