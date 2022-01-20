package com.paperfly.bigdata.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.language.implicitConversions

//读取内存中的数据，创建RDD
object Spark01_RDD_Operator_Transform_reduceByKey {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1),("a", 1), ("b", 2), ("c", 3)))

    //第二个参数，代表分区数量
    rdd.reduceByKey(_ + _, 2).collect().foreach(println)


    //todo 关闭RDD
    sc.stop()
  }

}
