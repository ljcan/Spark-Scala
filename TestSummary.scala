package cn.just.shinelon.MLlib.Statistics


import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求数据的均值与标准差
  * 测试数据格式如下：
  * 1
    2
    3
    4
    5
    6
    7
    8
    9
  结果如下：
  [5.0]
  [7.5]

  曼哈段距离：x1+x2+x3+...+xn
  欧几里得距离：根号下x1^2+x2^2+.....+xn^2

  *
  */
object TestSummary {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setAppName("TestSummary")
          .setMaster("local")

    val sc=new SparkContext(conf)

    val rdd=sc.textFile("C:\\Users\\shinelon\\Desktop\\Statistics.txt")
          .map(_.split(" ").map(_.toDouble))
          .map(line=>Vectors.dense(line))       //转换为向量格式
    //将所有的RDD聚合在一起，求出 总和
    val summary=Statistics.colStats(rdd)    //获取Statistics实例
    println(summary.mean)                  //计算均值
    println(summary.variance)              //计算标准差
    println("曼哈段距离为: "+summary.normL1)         //计算曼哈段距离
    println("欧几里得距离为: "+summary.normL2)         //计算欧几里得距离
  }

  /**
    * 源码如下：
    *Statistics：
    *  @Since("1.1.0")
  def colStats(X: RDD[Vector]): MultivariateStatisticalSummary = {
    //实例化一个行矩阵然后进行计算
    new RowMatrix(X).computeColumnSummaryStatistics()
  }

    RowMatrix：
  @Since("1.0.0")
  def computeColumnSummaryStatistics(): MultivariateStatisticalSummary = {
    val summary = rows.treeAggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
    updateNumRows(summary.count)
    summary
  }
  @Since("1.0.0")
  def computeColumnSummaryStatistics(): MultivariateStatisticalSummary = {
    val summary = rows.treeAggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
    updateNumRows(summary.count)
    summary
  }
MultivariateStatisticalSummary：
/**
    * Add a new sample to this summarizer, and update the statistical summary.
    *
    * @param sample The sample in dense/sparse vector format to be added into this summarizer.
    * @return This MultivariateOnlineSummarizer object.
    */
  @Since("1.1.0")
  def add(sample: Vector): this.type = add(sample, 1.0)

  private[spark] def add(instance: Vector, weight: Double): this.type = {
    require(weight >= 0.0, s"sample weight, ${weight} has to be >= 0.0")
    if (weight == 0.0) return this

    if (n == 0) {
      require(instance.size > 0, s"Vector should have dimension larger than zero.")
      n = instance.size

      currMean = Array.ofDim[Double](n)
      currM2n = Array.ofDim[Double](n)
      currM2 = Array.ofDim[Double](n)
      currL1 = Array.ofDim[Double](n)
      nnz = Array.ofDim[Double](n)
      currMax = Array.fill[Double](n)(Double.MinValue)
      currMin = Array.fill[Double](n)(Double.MaxValue)
    }

    require(n == instance.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $n but got ${instance.size}.")

    val localCurrMean = currMean
    val localCurrM2n = currM2n
    val localCurrM2 = currM2
    val localCurrL1 = currL1
    val localNnz = nnz
    val localCurrMax = currMax
    val localCurrMin = currMin
    instance.foreachActive { (index, value) =>
      if (value != 0.0) {
        if (localCurrMax(index) < value) {
          localCurrMax(index) = value
        }
        if (localCurrMin(index) > value) {
          localCurrMin(index) = value
        }

        val prevMean = localCurrMean(index)
        val diff = value - prevMean
        localCurrMean(index) = prevMean + weight * diff / (localNnz(index) + weight)
        localCurrM2n(index) += weight * (value - localCurrMean(index)) * diff
        localCurrM2(index) += weight * value * value
        localCurrL1(index) += weight * math.abs(value)

        localNnz(index) += weight
      }
    }

    weightSum += weight
    weightSquareSum += weight * weight
    totalCnt += 1
    this
  }

/**
    * Merge another MultivariateOnlineSummarizer, and update the statistical summary.
    * (Note that it's in place merging; as a result, `this` object will be modified.)
    *
    * @param other The other MultivariateOnlineSummarizer to be merged.
    * @return This MultivariateOnlineSummarizer object.
    */
  @Since("1.1.0")
  def merge(other: MultivariateOnlineSummarizer): this.type = {
    if (this.weightSum != 0.0 && other.weightSum != 0.0) {
      require(n == other.n, s"Dimensions mismatch when merging with another summarizer. " +
        s"Expecting $n but got ${other.n}.")
      totalCnt += other.totalCnt
      weightSum += other.weightSum
      weightSquareSum += other.weightSquareSum
      var i = 0
      while (i < n) {
        val thisNnz = nnz(i)
        val otherNnz = other.nnz(i)
        val totalNnz = thisNnz + otherNnz
        if (totalNnz != 0.0) {
          val deltaMean = other.currMean(i) - currMean(i)
          // merge mean together
          currMean(i) += deltaMean * otherNnz / totalNnz
          // merge m2n together
          currM2n(i) += other.currM2n(i) + deltaMean * deltaMean * thisNnz * otherNnz / totalNnz
          // merge m2 together
          currM2(i) += other.currM2(i)
          // merge l1 together
          currL1(i) += other.currL1(i)
          // merge max and min
          currMax(i) = math.max(currMax(i), other.currMax(i))
          currMin(i) = math.min(currMin(i), other.currMin(i))
        }
        nnz(i) = totalNnz
        i += 1
      }
    } else if (weightSum == 0.0 && other.weightSum != 0.0) {
      this.n = other.n
      this.currMean = other.currMean.clone()
      this.currM2n = other.currM2n.clone()
      this.currM2 = other.currM2.clone()
      this.currL1 = other.currL1.clone()
      this.totalCnt = other.totalCnt
      this.weightSum = other.weightSum
      this.weightSquareSum = other.weightSquareSum
      this.nnz = other.nnz.clone()
      this.currMax = other.currMax.clone()
      this.currMin = other.currMin.clone()
    }
    this
  }
    */



}
