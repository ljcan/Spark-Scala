package cn.just.shinelon.MLlib.Algorithm

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * MLLib线性回归实战：商品价格与消费者收入之间的关系
  *
  * 5,1 1
    7,2 1
    9,3 2
    11,4 1
    19,5 3
    18,6 2
  */
object LinearRegression02 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("LinearRegression02")
    val sc = new SparkContext(conf)

    val datas = sc.textFile("C:\\Users\\shinelon\\Desktop\\LinearRegression02.txt")

    val parseDatas = datas.map(line => {
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble))
      )
    }).cache() //转换为数据格式（RDD）

    //建立模型
    val model = LinearRegressionWithSGD.train(parseDatas, 10000, 0.1) //数据，迭代次数，步进系数

    println("model weights:")
    println(model.weights)
    //通过模型预测模型
    val result = model.predict(Vectors.dense(7,600))
    println(result)

    val valuesAndPreds = parseDatas.map(point => { //获取真实值与预测值
      val prediction = model.predict(point.features) //对系数进行预测
      (point.label, prediction) //按格式进行存储
    })

    //均方误差
    val MSE = valuesAndPreds.map { case (v, p) =>
      math.pow((v - p), 2)
    }.mean()
      println(MSE)

  }

}
