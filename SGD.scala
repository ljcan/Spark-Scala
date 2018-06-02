package cn.just.shinelon.MLlib.Algorithm

import java.util

import scala.collection.immutable.HashMap

/**
  * 随机梯度下降算法实战
  * 随机梯度下降算法：最短路径下达到最优结果
  * 数学表达公式如下：
  * f(θ)=θ0x0+θ1x1+θ2x2+...+θnxn
  * 对于系数要通过不停地求解出当前位置下最优化的额数据，即不停对系数θ求偏导数
  * 则θ求解的公式如下：
  * θ=θ-α(f(θ)-yi)xi
  * 公式中α是下降系数，即每次下降的幅度大小，系数越大则差值越小，系数越小则差值越小，但是计算时间也相对延长
  *
  * ??
  * 随着步长系数增大以及数据量的增大，为什么θ值偏差越来越大，当数据量大到一定程度的时候，为什么θ值会为NaN
  */
object SGD {
  var data=HashMap[Int,Int]()         //创建数据集
  def getdata():HashMap[Int,Int]={
    for(i <- 1 to 50){                //创建50个数据集
      data += (i->(2*i))              //写入公式y=2x
    }
    data                              //返回数据集
  }

  var θ:Double=0                        //第一步 假设θ为0
  var α:Double=0.1                      //设置步进系数

  def sgd(x:Double,y:Double)={        //随机梯度下降迭代公式
    θ=θ-α*((θ*x)-y)                 //迭代公式
  }

  def main(args: Array[String]): Unit = {
    val dataSource=getdata()          //获取数据集
    dataSource.foreach(myMap=>{       //开始迭代
      sgd(myMap._1,myMap._2)          //输入数据
    })
    println("最终结果值θ为："+θ)
  }
}
