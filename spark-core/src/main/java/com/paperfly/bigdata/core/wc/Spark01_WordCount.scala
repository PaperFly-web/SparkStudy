package com.paperfly.bigdata.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //todo 建立和spark框架的连接
    var sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO 执行业务逻辑

    //TODO 关闭连接
    sc.stop()
  }

}
