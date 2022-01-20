package com.paperfly.bigdata.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//glom
object Spark01_RDD_Operator_Transform_Glom {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD

    val rdd: RDD[Int] = sc.makeRDD(List(
      1, 2, 3, 4
    ), 2)

    //List=>Int
    //Int=>Array
    //将集合，转换为数组
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    //实现，打印每个分区的数据，用逗号分隔
    glomRDD.map(data=>{
      //将数组按照分隔符拼接起来
      println(data.mkString(","))
    }).collect()

    //实现，把每个分区最大值求出来，然后求和
    println(glomRDD.map(data => {
      data.max
    }).collect().sum)


    //todo 关闭RDD
    sc.stop()
  }

}
