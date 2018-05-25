package cn.just.shinelon.MLlib.Statistics

import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.mllib.stat.Statistics

/**
  * 假设检验
  * 卡方检验：一种常用的假设检验方法，能够较好的对数据集之间的拟合度，相关性和独立性进行验证。
  * MLlib规定常用的卡方检验使用的数据集一般为向量和矩阵
  * 卡方检验使用了皮尔逊计算法对数据集进行计算，得到最终的结果P值。
  * 一般情况下，P<0.05是指数据集不存在显著性差异
  *
  * 假定检测的基本思路是，首先我们假定一个结论，然后为这个结论设置期望值，
  * 用实际观察值来与这个值做对比，并设定一个阀值，如果计算结果大于阀值，则假定不成立，否则成立。
  *
  */
object TestChiSq {
  def main(args: Array[String]): Unit = {
    val vd=Vectors.dense(1,2,3,4,5)
    val vdResult=Statistics.chiSqTest(vd)
    println(vdResult)
    println("-----------------------------------------")
    val mtx=Matrices.dense(3,2,Array(1,3,5,2,4,6))
    val mtxResult=Statistics.chiSqTest(mtx)
    println(mtxResult)

    /**
      * 打印结果如下：
      * Chi squared test summary:
        method: pearson               //卡方检验使用方法
        degrees of freedom = 4        //自由度：总体参数估计量中变量值独立自由变化的数目
        statistic = 3.333333333333333   //统计量：不同方法下的统计量
        pValue = 0.5036682742334986     //P值：显著性差异指标
        No presumption against null hypothesis: observed follows the same distribution as expected..
        -----------------------------------------
        Chi squared test summary:
        method: pearson
        degrees of freedom = 2
        statistic = 0.14141414141414144
        pValue = 0.931734784568187
        No presumption against null hypothesis: the occurrence of the outcomes is statistically independent..
      */
  }
}
