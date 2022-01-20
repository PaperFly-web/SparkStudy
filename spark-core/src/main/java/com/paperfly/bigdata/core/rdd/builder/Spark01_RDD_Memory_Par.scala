package com.paperfly.bigdata.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

//测试并行分区
object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //可以在配置文件中，设置并行分区个数
    //conf.set("spark.default.parallelism","5")
    val sc = new SparkContext(conf)

    //TODO 创建RDD
    val seq = Seq[Int](1, 2, 3, 4)
    //指定分区数量为5
    val rdd: RDD[Int] = sc.makeRDD(seq,5)
    //
    rdd.saveAsTextFile("output")//保存到文件中

    //todo 关闭RDD
    sc.stop()
  }
}
