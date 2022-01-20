package com.paperfly.bigdata.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.language.implicitConversions

//读取内存中的数据，创建RDD
object Spark01_RDD_Operator_Transform_partitionBy {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)

    rdd.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")
    rdd.collect()


    //todo 关闭RDD
    sc.stop()
  }

}
