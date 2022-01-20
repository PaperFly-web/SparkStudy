package com.paperfly.bigdata.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//glom
object Spark01_RDD_Operator_Transform_Sample {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD

    val rdd: RDD[Int] = sc.makeRDD(List(
      1, 2, 3, 4,5,6,7,8
    ), 2)

    // 抽取数据放回（泊松算法）
    // 第一个参数：抽取的数据是否放回， true：放回； false：不放回
    // 第二个参数：重复数据的几率，范围大于等于 0.表示每一个元素被期望抽取到的次数
    // 第三个参数：随机数种子
    println(rdd.sample(
      true,
      2
    ).collect().mkString(","))

    // 抽取数据不放回（伯努利算法）
    // 伯努利算法：又叫 0、 1 分布。例如扔硬币，要么正面，要么反面。
    // 具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不  要
    // 第一个参数：抽取的数据是否放回， false：不放回
    // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取； 1：全取；
    // 第三个参数：随机数种子
    rdd.sample(
      false,
      0.4,
      1
    )

    //todo 关闭RDD
    sc.stop()
  }

}
