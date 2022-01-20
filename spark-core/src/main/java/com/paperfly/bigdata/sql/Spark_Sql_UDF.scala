package com.paperfly.bigdata.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark_Sql_UDF {

  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSql")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //导入隐式转换
    import spark.implicits._

    //1.从json文件创建DataFrame
    val df: DataFrame = spark.read.json("data/user.json")

    spark.udf.register("prefixName", (name: String) => {
      prefixName(name)
    })

    df.createOrReplaceTempView("user")

    spark.sql("select age,prefixName(username) from user").show()


    //TODO 关闭SparkSql
    spark.close()
  }

  def prefixName(name: String): String = {
    "PaperFly:" + name
  }
}


