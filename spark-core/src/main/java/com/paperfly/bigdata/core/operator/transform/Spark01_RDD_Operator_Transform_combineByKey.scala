package com.paperfly.bigdata.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.implicitConversions

//读取内存中的数据，创建RDD
object Spark01_RDD_Operator_Transform_combineByKey {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD

    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93),
      ("a", 95), ("b", 98))
    val input: RDD[(String, Int)] = sc.makeRDD(list, 2)
    val combineRdd: RDD[(String, (Int, Int))] = input.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    combineRdd.map(value=>{
      (value._1,(value._2._1/value._2._2))
    }).collect().foreach(println)


    //todo 关闭RDD
    sc.stop()
  }

}
