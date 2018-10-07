package cn.just.shinelon.MLlib.Algorithm


import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object LogisticRegression01 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setAppName("LogisticRegression01")
           .setMaster("local")

    val sc=new SparkContext(conf)

    val data=sc.textFile("C:\\Users\\shinelon\\Desktop\\LogtisRegression01.txt")


    /**
      * case class LabeledPoint(label: Double, features: Vector) {
        override def toString: String = {
          "(%s,%s)".format(label, features)
        }
      }
      */
    val parsedData=data.map{line=>
      val parts = line.split('|')
      val vs: linalg.Vector=Vectors.dense(parts(1).toDouble)
      LabeledPoint(parts(0).toDouble,vs)
    }.cache()



    val model=LogisticRegressionWithSGD.train(parsedData,50)        //建立模型

    val target=Vectors.dense(-1)                    //创建测试值

    val result=model.predict(target)                //根据模型计算结果
    println(result)
  }
}
