package com.paperfly.bigdata.core.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//读取内存中的数据，创建RDD
object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD

    val rdd2: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
    println(rdd2.countByKey())

    val rdd: RDD[Int] = sc.makeRDD(List(
      1, 2, 3, 4, 5, 6, 1, 2, 3, 2, 1
    ), 3)

    val value = rdd.map(
      v => {
        (v, 1)
      }
    )

    println(rdd.countByValue())

    println(rdd.fold(0)(
      (acc, v) => acc + v
    ))

    println(rdd.aggregate(0)(
      (acc, v) => acc + v,
      (acc, v) => acc + v
    ))

    println(rdd.takeOrdered(5).mkString(","))

    println(rdd.take(3).mkString(","))

    println(rdd.first())

    println(rdd.count())

    println(rdd.collect().mkString(","))

    println(rdd.reduce(_ + _))



    //todo 关闭RDD
    sc.stop()
  }

}
