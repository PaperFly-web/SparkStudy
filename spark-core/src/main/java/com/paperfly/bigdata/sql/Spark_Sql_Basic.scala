package com.paperfly.bigdata.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark_Sql_Basic {

  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSql")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //导入隐式转换
    import spark.implicits._
    //TODO  创建DataFrame
    //1.从json文件创建DataFrame
    val df: DataFrame = spark.read.json("data/user.json")
    //df.show()

    // DataFrame => SQL
    //创建临时视图（存在这个视图就替换，不存在就创建）
    df.createOrReplaceTempView("user")
    spark.sql("select age from user").show()
    spark.sql("select avg(age) from user").show()


    // DataFrame => DSL
    df.select("age").show()
    // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    df.select($"age" + 1).show()
    df.select('age + 1).show()


    // TODO DataSet
    // DataFrame其实是特定泛型的DataSet
    var seq = Seq(1, 2, 3, 4)
    val ds: Dataset[Int] = seq.toDS()
    ds.show()

    //RDD <=> DataFrame
    val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    //    rdd.toDF("id","name","age").show()
    val df2: DataFrame = rdd.toDF("id", "name", "age")
    val rdd1: RDD[Row] = df2.rdd

    // DataFrame <=> DataSet
    val ds2: Dataset[User] = df.as[User]
    val df1: DataFrame = ds2.toDF()

    // RDD <=> DataSet
    val ds3: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val rdd2: RDD[User] = ds3.rdd

    //TODO 关闭SparkSql
    spark.close()
  }

  case class User(id: Int, name: String, age: Int)
}
