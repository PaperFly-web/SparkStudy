package com.paperfly.bigdata.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

object Spark_Sql_UDAF {

  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSql")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //导入隐式转换

    //1.从json文件创建DataFrame
    val df: DataFrame = spark.read.json("data/user.json")

    spark.udf.register("myAvg", functions.udaf(new MyAvgUDAF()))

    df.createOrReplaceTempView("user")

    spark.sql("select myAvg(age) from user").show()


    //TODO 关闭SparkSql
    spark.close()
  }

  case class Buff(var total: Long, var count: Long)

  /*
 自定义聚合函数类：计算年龄的平均值
 1. 继承org.apache.spark.sql.expressions.Aggregator, 定义泛型
     IN : 输入的数据类型 Long
     BUF : 缓冲区的数据类型 Buff
     OUT : 输出的数据类型 Long
 2. 重写方法(6)
 */
  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
    // z & zero : 初始值或零值
    // 缓冲区的初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    //进行聚合
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total += in
      buff.count += 1
      buff
    }

    //进行多个分区合并
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total += buff2.total
      buff1.count += buff2.count
      buff1
    }

    ////计算结果
    override def finish(buff: Buff): Long = {
      buff.total / buff.count
    }

    // 缓冲区的编码操作.固定写法：自定义的类型就是 Encoders.product.     scala的数据类型，就是Encoders.scalaXXXX
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
