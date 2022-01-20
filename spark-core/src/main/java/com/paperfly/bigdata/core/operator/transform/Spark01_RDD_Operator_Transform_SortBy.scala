package com.paperfly.bigdata.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//读取内存中的数据，创建RDD
object Spark01_RDD_Operator_Transform_SortBy {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 2), ("6", 3)), 2)

    //第一个参数：按照tuple的第一个元素的转换为Int后
    // 第二个参数：按照降序排序
    rdd.sortBy(t=>t._1.toInt,false).collect().foreach(println)


    //todo 关闭RDD
    sc.stop()
  }

}
