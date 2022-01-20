package com.paperfly.bigdata.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//读取内存中的数据，创建RDD
object Spark01_RDD_Operator_Transform_MapPartitions {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD
    //准备内存中的数据
    val seq = Seq[Int](1, 2, 3, 4)
    //    val rdd: RDD[Int] = sc.parallelize(seq)同下行功能
    val rdd: RDD[Int] = sc.makeRDD(seq)
    //
    rdd.mapPartitions(iter => {
      List(iter.max).iterator
    }).foreach(println)

    val res: Array[Int] = rdd.collect()

    //todo 关闭RDD
    sc.stop()
  }

}
