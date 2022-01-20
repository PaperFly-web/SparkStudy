package com.paperfly.bigdata.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

import java.util.Properties

object Spark_Sql_JDBC {

  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSql")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //导入隐式转换
    import spark.implicits._
    //方式 1：通用的 load 方法读取
    spark.read.format ("jdbc")
      .option ("url", "jdbc:mysql://localhost:3306/my_class?useUnicode=true&characterEncoding=utf-8&useSSL=true&serverTimezone=UTC")
      .option ("driver", "com.mysql.cj.jdbc.Driver")
      .option ("user", "root")
      .option ("password", "HJC916924")
      .option ("dbtable", "class")
      .load ().show

    //方式 2:通用的 load 方法读取 参数另一种形式
    spark.read.format ("jdbc")
      .options (Map ( "url" ->"jdbc:mysql://localhost:3306/my_class?user=root&password=HJC916924&useUnicode=true&characterEncoding=utf-8&useSSL=true&serverTimezone=UTC",
        "dbtable" -> "class",
        "driver" -> "com.mysql.cj.jdbc.Driver") ).load ().show
    //方式 3:使用 jdbc 方法读取
    val props: Properties = new Properties ()
    props.setProperty ("user", "root")
    props.setProperty ("password", "HJC916924")
    val df: DataFrame = spark.read.jdbc ("jdbc:mysql://localhost:3306/my_class?useUnicode=true&characterEncoding=utf-8&useSSL=true&serverTimezone=UTC", "class", props)
    df.show

    //TODO 关闭SparkSql
    spark.close()
  }


}
