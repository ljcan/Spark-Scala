package main.scala.cn.shinelon.com

/**
  * 二次排序使用的key
  * @param frist
  * @param second
  */
class DoubleSortKey(val frist: Int,val second: Int) extends Ordered[DoubleSortKey] with Serializable {
  override def compare(that: DoubleSortKey) :Int= {
    if (this.frist - that.frist != 0) {
      this.frist - that.frist
    }else{
      this.second-that.second
    }
  }
}
