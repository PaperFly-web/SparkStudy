package com.paperfly.bigdata.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//用groupBy实现，wordCount
object Spark01_RDD_Operator_Transform_GroupBy {
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
    val groupByRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(s => {
      //1.进行数据分割
      s.split(" ")
    }).flatMap(list => {
      //2.数据分割后，变成二维集合，所以要扁平化一次
      list
    }).map(s => {
      //3.数据整理映射
      (s, 1)
    }).groupBy(tuple => {
      //4.根据单词进行分组
      tuple._1//_1是元组的访问第一个元素的用法
    })

    //5.个时候groupByRDD的数据类型是RDD[(String, Iterable[(String, Int)])]
    groupByRDD.map(g=>{
      //取出每组的key，然后取出第二个元素的大小
      (g._1,g._2.size)
    }).collect().foreach(println)


    //todo 关闭RDD
    sc.stop()
  }

}
