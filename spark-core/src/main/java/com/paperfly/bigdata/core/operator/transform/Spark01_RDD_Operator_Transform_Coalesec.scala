package com.paperfly.bigdata.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//读取内存中的数据，创建RDD
object Spark01_RDD_Operator_Transform_Coalesec {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD

    val rdd: RDD[Int] = sc.makeRDD(List(
      1, 2, 3, 4, 5, 6
    ), 3)

    /*//将3个分区，合并为2个分区
    //第二个参数
    //     如果设置为true（默认false）,会把所有数据都打散重新分区
    //     如果为false,那么默认不把分区的数据拆分
    val coalesceRDD: RDD[Int] = rdd.coalesce(2, true)
    coalesceRDD.saveAsTextFile("output")
    rdd.collect()*/

    //扩大分区
    //如果要扩大分区，那么第二个参数一定要传，且必须为true
    //如果不进行shuffle的话，默认是不会把分区内的数据进行拆分的，那么就没办法进行扩大分区
    rdd.coalesce(4,true).saveAsTextFile("output")
    //同上功能
//    rdd.repartition(4)


    //todo 关闭RDD
    sc.stop()
  }

}
