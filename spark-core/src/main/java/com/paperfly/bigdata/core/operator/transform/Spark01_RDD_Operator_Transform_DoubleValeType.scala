package com.paperfly.bigdata.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.implicitConversions

//读取内存中的数据，创建RDD
object Spark01_RDD_Operator_Transform_DoubleValeType {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3,4,5,6), 2)

    //交集,俩个集合数据类型必须一致
    //【3,4】
    println(rdd1.intersection(rdd2).collect().mkString(","))
    //并集,俩个集合数据类型必须一致【1,2,3,4,3,4,5,6】
    println(rdd1.union(rdd2).collect().mkString(","))
    //差集,俩个集合数据类型必须一致【1,2】
    println(rdd1.subtract(rdd2).collect().mkString(","))
    //拉链
    //    俩个集合数据类型必须一致
    //    俩个数据集合数量一致
    //     俩个数据集合分区一样
    // 【（1,3），（2,4）（3,5),(4,6)】
    println(rdd1.zip(rdd2).collect().mkString(","))



    implicit def asa(d: Double) = d.toInt

    val num: Int = 3.5 // 3

    //todo 关闭RDD
    sc.stop()
  }

}
