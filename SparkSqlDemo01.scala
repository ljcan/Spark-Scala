package main.scala.cn.shinelon.com


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext, sql}

object SparkSqlDemo01 {

  //使用case定义Schema（不能超过22个属性），实现Person接口
  //只有case类才能隐式转换为一个DataFrame
  case class Person(name: String,age: Int)

  def main(args: Array[String]): Unit = {
       val conf=new SparkConf()
      .setMaster("local")
      .setAppName("SparkSqlDemo01")

    val sc=new SparkContext(conf)

//    test01(sc)
//      test02(sc)
//      test03(sc)
//    testHive(sc)
//      testParquet(sc)
//      testReadParquet(sc)
//      parquetTable(sc)
    testJDBC(sc)
//    testPartition(sc)
  }


  def test01(sc:SparkContext): Unit ={
    val sqlContext=new sql.SQLContext(sc)
    val df=sqlContext.jsonFile("file:///F:/spark-2.0.0/SparkApp/src/cn/just/shinelon/txt/SparkSql01.json")

    df.registerTempTable("people")

    println(df.show())      //打印表数据

    println(df.printSchema())  //以树的形式打印DataFrame的Schema

    println(df.select(df("name"),df("age")+1).show())

    println("===============================")

    val result=sqlContext.sql("select name from people")

    result.foreach(println)
  }

  /**
    * 以反射机制推断RDD模式创建DataFrame
    * @param sc
    */
  def test02(sc:SparkContext): Unit ={
    val sqlContext=new sql.SQLContext(sc)

    import sqlContext.implicits._              //隐式转换，将一个RDD转换为DataFrame

    //使用前缀hdfs://来标识HDFS存储系统的文件
    val people=sc.textFile("file:///F:/spark-2.0.0/SparkApp/src/cn/just/shinelon/txt/SparkSql02.txt")
          .map(_.split(",")).map(p=>Person(p(0),p(1).trim.toInt)).toDF()

    //DataFrame注册临时表
    people.registerTempTable("person")
    //使用sql运行SQL表达式
    val result=sqlContext.sql("SELECT name,age from person WHERE age>=19")

    println(result.map(t=>"Name:"+t(0)).collect())

    println(result.map(t=>"Name:"+t.getAs[String](1)).collect())


  }

  /**
    * 以编程方式定义RDD模式
    * @param sc
    */
  def test03(sc:SparkContext): Unit ={
    val sqlContext=new sql.SQLContext(sc)

    val people=sc.textFile("file:///F:/spark-2.0.0/SparkApp/src/cn/just/shinelon/txt/SparkSql02.txt")

    val schemaString="name age"
    //生成基于schemaString结构的schema
    val schema=StructType(
      schemaString.split(" ").map(fieldName=>StructField(fieldName,DataTypes.StringType,true))
    )

    val rowRDD=people.map(_.split(",")).map(p=>Row(p(0),p(1).trim))

    //将自定义schema应用于RDD
    val peopleDF=sqlContext.createDataFrame(rowRDD,schema)

    //注册临时表
    peopleDF.registerTempTable("peoletable")

    //使用sql运行SQL表达式
    val result=sqlContext.sql("select name from peoletable")

    result.map(t=>"Name:"+t(0)).collect().foreach(println)

  }

  /**
    * Spark SQL集成Hive表
    * 在Spark-shell中可以运行
    * @param sc
    */
  def testHive(sc:SparkContext): Unit ={
    val hiveContext=new HiveContext(sc)


    //创建表
    hiveContext.sql("create table if not exists src (key int,value string)")
    //加载数据
    hiveContext.sql("load data local inpath 'file:///opt/cdh-5.3.6/spark-1.3.0-bin-2.5.0-cdh5.3.6/data/kv1.txt' into table src")
    //查询
    hiveContext.sql("from src select key,value").collect().foreach(println)

    /**
      * 运行部分结果如下：
      *
    [238,val_238]
    [86,val_86]
    [311,val_311]
    [27,val_27]
    [165,val_165]
    [409,val_409]
    [255,val_255]
      */

    sc.stop()
  }

  /**
    * 测试parquet数据源
    * 写数据
    * @param sc
    */
  def testParquet(sc:SparkContext): Unit ={

//    SaveMode.Ignore

    val sqlContext=new SQLContext(sc)

    val df=sqlContext.jsonFile("file:///F:/spark-2.0.0/SparkApp/src/cn/just/shinelon/txt/SparkSql01.json")

    println(df.show())      //打印表数据

    df.select("name","age").save("nameAndAge.parquet")
  }

  /**
    * parquet格式文件需要在集群上读写，在Windows本地进行操作会报错
    * 本地运行无法创建broadcast，因此会报空指针异常
    * @param sc
    */
  def testReadParquet(sc:SparkContext): Unit ={
    val sqlContext=new SQLContext(sc)

    val df=sqlContext.parquetFile("file:///F:/spark-2.0.0/SparkApp/nameAndAge.parquet")

    df.printSchema()

    println(df.show())
  }

  /**
    * 将普通文本文件转换为parquet数据源来创建临时表
    * @param sc
    */
  def parquetTable(sc:SparkContext): Unit ={
    val sqlContext=new SQLContext(sc)
    //隐式转换为一个DataFrame
    import sqlContext.implicits._

    val peopleDF=sc.textFile("file:///F:/spark-2.0.0/SparkApp/src/cn/just/shinelon/txt/SparkSql02.txt")
      .map(_.split(",")).map(p=>Person(p(0),p(1).trim.toInt)).toDF()

    peopleDF.saveAsParquetFile("hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/SparkSql/people.parquet")
    //加载Parquet为DataFrame
    val parquetFile=sqlContext.parquetFile("hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/SparkSql/people.parquet")
    //将DataFrame注册为临时表，提供SQL查询使用
    parquetFile.registerTempTable("parquetTable")

    val result=sqlContext.sql("select name from parquetTable")

    result.map(t=>"Name:"+t(0)).collect().foreach(println)

    /**
      * 运行结果如下：
      * Name:shinelon
        Name:mike
        Name:wangwu
      */
  }

  /**
    *
  scala> df1.printSchema()
root
 |-- single: integer (nullable = false)
 |-- double: integer (nullable = false)
    * @param sc
    */
  def testPartition(sc:SparkContext): Unit ={
    val sqlContext=new SQLContext(sc)
    //隐式转换
    import sqlContext.implicits._

    val df1=sc.makeRDD(1 to 5).map(i=>(i,i*2)).toDF("single","double")
    df1.saveAsParquetFile("data/test/key=1")

    df1.printSchema()

//    val df2=sc.makeRDD(6 to 10).map(i=>(i*3)).toDF("single","triple")
//    df2.saveAsParquetFile("data/test/key=2")


  }

  def testJDBC(sc:SparkContext): Unit ={
    val sqlContext=new SQLContext(sc)

    val jdbcDF=sqlContext.load("jdbc",Map(
      "url"->"jdbc:mysql://127.0.0.1:3306/library?user=root&password=123456",
      "dbtable"->"book",
      "driver"->"com.mysql.jdbc.Driver"
//      "user"->"root",
//      "password"->"123456"
    ))

    jdbcDF.show()
  }



}
