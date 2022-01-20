package com.paperfly.bigdata.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//读取内存中的数据，创建RDD
object Spark02_RDD_Acc1 {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4, 5, 6)
    )

    var sum = sc.longAccumulator("sum")
    rdd.foreach(
      num => {
        sum.add(num)
      }
    )
    println("sum=" + sum.value)

    //todo 关闭RDD
    sc.stop()
  }

}
