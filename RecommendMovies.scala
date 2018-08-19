package cn.just.shinelon.MLlib.project.Algorithm

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  *数据到https://movielens.org下载
  *
  * movies.dat中的数据格式：
  * MovieUD::Title::Genres
  *
  * ratings.dat中的数据格式：
  * UserID::MovieID::Rating::Timestamp
  */
object RecommendMovies {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setAppName("RecommendMovies")
          .setMaster("local[8]")

    val sc=new SparkContext(conf)
    /**
      * 读取电影信息到本地
      */
    val movies=sc.textFile("F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\MLlib\\project\\data\\movies.dat").map({
      lines=>{
        val fields=lines.split("::")
        (fields(0).toInt,fields(1))
      }
    }).collect().toMap

    //读取评分数据为RDD

    val ratings=sc.textFile("F:\\spark-2.0.0\\SparkApp\\src\\cn\\just\\shinelon\\MLlib\\project\\data\\ratings.dat").map({
      lines=>{
        val fields=lines.split("::")
        val rating=Rating(fields(0).toInt,fields(1).toInt,fields(2).toDouble)     //UserId,MovieId,Rating
        val timestamp=fields(3).toLong%10
        (timestamp,rating)
      }
    })

    //输出数据集的基本信息
    val numRatings=ratings.count()                                  //评分数
    val numUsers=ratings.map(_._2.user).distinct().count()          //去重计算user的用户数
    val numMovies=ratings.map(_._2.product).distinct().count()      //获取电影数目
    println("Got "+numRatings+" rating from "+numUsers+" users on "+numMovies+" movies.")

    /**
      * 利用timestamp将数据集分为训练集、验证集和测试集
      */
      //将处理后的timestamp<6的划分为训练集
    val training=ratings.filter(x=>x._1<6).values.repartition(4).cache()
      //将timestamp大于等于6小于8的划分为验证集
    val validation=ratings.filter(x=>x._1>=6&&x._1<8).values.repartition(4).cache()
      //将timestamp大于8的划分为测试集
    val test=ratings.filter(x=>x._1>=8).values.cache()

    val numTraining=training.count()
    val numValidation=validation.count()
    val numTest=test.count()
    println("Training :"+numTraining+", Validation :"+numValidation+", Test :"+numTest)

    /**
      *使用不同的参数训练协同过滤模型，并且选出RMSE最小的模型
      * 矩阵分解的秩从8-12中选择，正则系数从1.0-10.0中选择，迭代次数从12-20中选择，共计8个模型
      */

      /**

    val ranks=List(8,12)              //模型中隐藏因子数
    val lambdas=List(1.0,10.0)        //ALS中的正则化系数
    val nubItems=List(10,20)          //迭代次数
    var bestModel:Option[MatrixFactorizationModel]=None     //初始化最佳模型
    var bestValidationRESM=Double.MaxValue                  //初始化最佳RMSE
    var bestRank=0
    var bestLambda= -1.0
    var bestNubItem= -1
    for(rank<-ranks;lambda<-lambdas;nubItem<-nubItems){
      val model=ALS.train(training,rank,nubItem,lambda)     //训练模型
      val validationRMSE=computeRMSE(model,validation)      //验证模型
      if(validationRMSE<bestValidationRESM){                //计算最佳RMSE
        bestModel=Some(model)                               //模型不为空
        bestValidationRESM=validationRMSE
        bestRank=rank
        bestLambda=lambda
        bestNubItem=nubItem
      }
    }
    val testRMSE=computeRMSE(bestModel.get,test)           //测试数据
    println("The best model was trained with rank = "+bestRank+" and lambda ="+bestLambda
      +", and numIter = "+bestNubItem+", and its RMSE on the test is "+testRMSE+".")

        */

    /**
      * 计算使用协同过滤算法和不使用协同过滤算法能得到多大的预测效果提升
      */
      val meanR=training.union(validation).map(_.rating).mean()     //评分平均值
    //不适用协同过来算法的均方误差
      val baseRmse=math.sqrt(test.map(x=>(meanR-x.rating)*(meanR-x.rating)).mean)

      val improvement=(baseRmse-0.880849)/baseRmse*100

      println("The best model improves the baseline by "+"%1.2f".format(improvement)+"%.")



    var bestModel:Option[MatrixFactorizationModel]=None
    //训练得到的最佳模型
    val model=ALS.train(training,8,20,10.0)

    bestModel=Some(model)

    /**
      * 利用训练得到的最佳模型为用户推荐前10的电影
      */
//    val myRatedMovieIds=test.map(_.product).toSet
//    val cands=sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
    val recommendM=bestModel.get.recommendProducts(1,10).sortBy(_.rating)

    val map=new mutable.HashMap[Int,Double]()

    recommendM.foreach(x=>{
//      map+=(x.product->x.rating)
      //获取电影名
      println(x.product+" : "+x.rating)
    })

    //java.lang.StackOverflowError
//    println("=========================================")
//
//    for((key,value)<-map){
//      println(movies(key)+":"+value)
//    }

  }

  /**
    * 定义函数计算均方误差
    */
  def computeRMSE(model:MatrixFactorizationModel,data:RDD[Rating]):Double={
    val moviesInfo=data.map(x=>((x.user,x.product),x.rating))
    val predictions:RDD[Rating]=model.predict(data.map(x=> (x.user,x.product)))
    val predictionsAndRating=predictions.map{x=>
      ((x.user,x.product),x.rating)
    }.join(moviesInfo).values                          //联合查询，求出其值
    val rmse=math.sqrt(predictionsAndRating.map(x=>(x._1-x._2)*(x._1-x._2)).mean())
    rmse
  }
}
