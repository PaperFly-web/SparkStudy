package com.paperfly.bigdata.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//读取内存中的数据，创建RDD
object Spark01_RDD_Operator_Transform_FlatMap {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD

    val rdd: RDD[String] = sc.makeRDD(List(
      "hello scala","hello spark"
    ))

    //扁平化，返回集合，最终收集的时候，把集合合并
    rdd.flatMap(s=>{
      s.split(" ")
    }).foreach(println)

    rdd.collect()

    //todo 关闭RDD
    sc.stop()
  }

}
