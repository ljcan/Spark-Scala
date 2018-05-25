package cn.just.shinelon.MLlib.Statistics

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 分层抽样
  * 在MLlib中，使用Map作为分层抽样的数据标记，一般情况下，Map的构成是[key,value]格式，key作为数据组，而value作为数据标签进行处理。
  * 测试数据如下：
  * aa
    bb
    aaa
    bbb
    ccc
  */
object TestStratifiedSampling {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
          .setAppName("TestStratifiedSampling")
          .setMaster("local")

    val sc=new SparkContext(conf)

    val data=sc.textFile("C:\\Users\\shinelon\\Desktop\\c.txt")
          .map{
            row=>{
              if(row.length==3)
                (row,1)
              else
                (row,2)
            }
          }.map(each=>(each._2,each._1))
    data.foreach(println)
    //设定抽样格式,将数据按照字符串长度分为了1,2两层，Map(1->0.5)表示在1层中抽取百分之五十
    //注意在按照概率分层抽样的时候每层抽取的概率综合必须为1，否则抽取失败
    val fractions:Map[Int,Double] = Map((1->0.8),(2->0.2))

    //withReplacement为false表示不重复抽样
    val approxSimple=data.sampleByKey(withReplacement = false,fractions,0)    //计算抽样样本

    approxSimple.foreach(println)
  }
}

/**
  * 打印结果如下：
  * (2,aa)
    (2,bb)
    (1,aaa)
    (1,bbb)
    (1,ccc)

  (2,bb)
  (1,aaa)
  (1,ccc)
  *
  *
  *
  *
  *
  *
  * 源码如下
  * Return a subset of this RDD sampled by key (via stratified sampling).
  *
  * Create a sample of this RDD using variable sampling rates for different keys as specified by
  * `fractions`, a key to sampling rate map, via simple random sampling with one pass over the
  * RDD, to produce a sample of size that's approximately equal to the sum of
  * math.ceil(numItems * samplingRate) over all key values.
  *
  * @param withReplacement whether to sample with or without replacement
  * @param fractions map of specific keys to sampling rates
  * @param seed seed for the random number generator
  * @return RDD containing the sampled subset
  def sampleByKey(withReplacement: Boolean,
      fractions: Map[K, Double],
      seed: Long = Utils.random.nextLong): RDD[(K, V)] = self.withScope {

    require(fractions.values.forall(v => v >= 0.0), "Negative sampling rates.")

    val samplingFunc = if (withReplacement) {
      StratifiedSamplingUtils.getPoissonSamplingFunction(self, fractions, false, seed)
    } else {
      StratifiedSamplingUtils.getBernoulliSamplingFunction(self, fractions, false, seed)
    }
    self.mapPartitionsWithIndex(samplingFunc, preservesPartitioning = true)
  }
  */
