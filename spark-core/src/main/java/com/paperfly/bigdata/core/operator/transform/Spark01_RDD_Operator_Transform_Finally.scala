package com.paperfly.bigdata.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.implicitConversions

//读取内存中的数据，创建RDD
object Spark01_RDD_Operator_Transform_Finally {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD

    val logRdd: RDD[String] = sc.textFile("data/log.txt")


    logRdd.map(s=>{
      //1.拆分成（(省份，广告),1）
      val splits: Array[String] = s.split(" ")
      ((splits(1),splits(3)),1)
      //2.聚合，把（省份，广告）相同的key进行聚合
    }).reduceByKey(_+_)
      //3.拆分成（省份（广告，数量））
      .map(value=>{
        (value._1._1,(value._1._2,value._2))
        //4.按照省份，进行分组
      }).groupByKey()
      .mapValues(values=>{
        //5.把可迭代集合，转换成List，然后按照升序排序，取前三个
        values.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }).collect().foreach(println)


    //todo 关闭RDD
    sc.stop()
  }

}
