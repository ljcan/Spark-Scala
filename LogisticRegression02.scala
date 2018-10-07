package cn.just.shinelon.MLlib.Algorithm

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object LogisticRegression02 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setMaster("local")
          .setAppName("LogisticRegression02")

    val sc=new SparkContext(conf)

    val data=MLUtils.loadLibSVMFile(sc,"F:\\spark-2.0.0\\spark-2.0.0\\data\\mllib\\sample_libsvm_data.txt")
    //训练数据
    val model=LogisticRegressionWithSGD.train(data,50)
    //打印个数
    println(model.weights.size)
    //打印值
    println(model.weights)
    //打印不为0的个数
    println(model.weights.toArray.filter(_!=0).size)
  }
}
