package cn.just.shinelon.MLlib.Algorithm

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 线性回归
  * y=2x1+3x2
  *
  * 5,1 1
    7,2 1
    9,3 2
    11,4 1
    19,5 3
    18,6 2
  */
object LinearRegression {
  val conf=new SparkConf()
        .setAppName("LinearRegression")
        .setMaster("local")

  val sc=new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val data=sc.textFile("C:\\Users\\shinelon\\Desktop\\e.txt")

    val rdd=data.map(num=>{
      val parseData=num.split(",")                            //根据逗号进行分区
      LabeledPoint(parseData(0).toDouble,                     //提取因变量
        Vectors.dense(parseData(1).split(" ").map(_.toDouble)))  //提取自变量并且转换为向量
    }).cache()                                                //转化为相应的数据格式（LabeledPoint格式的数据）
    //建模
    val model=LinearRegressionWithSGD.train(rdd,100,0.1)
    val result=model.predict(Vectors.dense(5,2))               //通过模型预测模型
    println("结果为："+result)
    println("weight:"+model.weights)

    val valueAndPres=rdd.map(point=>{                        //获取真实值与预测值
      val res=model.predict(point.features)                  //对系数进行预测
      (point.label,res)
    })

    val MSE=valueAndPres.map{                                //计算均方误差（MSE)
      case(v,p)=>math.pow((v - p),2)
    }.mean()

    println("MSE:"+MSE)



  }
}
