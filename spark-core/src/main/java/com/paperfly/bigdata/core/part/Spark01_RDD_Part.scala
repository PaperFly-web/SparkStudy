package com.paperfly.bigdata.core.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

//读取内存中的数据，创建RDD
object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD
    val rdd: RDD[(String, String)] = sc.makeRDD(List(("nba", "sdaasda"), ("cba", "nigdndjf"), ("wnba", "gfdghfdh"), ("nba", "51fe5s")))
    val partRdd: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

    partRdd.saveAsTextFile("output")


    //todo 关闭RDD
    sc.stop()
  }

  class MyPartitioner extends Partitioner {
    //设置分区数量
    override def numPartitions: Int = 3

    //自定义分区规则
    // 根据数据的key值返回数据所在的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "cba" => 1
        case _ => 2
      }
    }
  }
}
