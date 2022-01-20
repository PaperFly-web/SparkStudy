package com.paperfly.bigdata.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.implicitConversions

//读取内存中的数据，创建RDD
object Spark01_RDD_Operator_Transform_aggregateByKey {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    // TODO : 取出每个分区内相同 key 的最大值然后分区间相加
    // aggregateByKey 算子是函数柯里化，存在两个参数列表
    // 1. 第一个参数列表中的参数表示初始值
    // 2. 第二个参数列表中含有两个参数
    // 2.1 第一个参数表示分区内的计算规则
    // 2.2 第二个参数表示分区间的计算规则
    /*rdd.aggregateByKey(10)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)*/
    val aggRdd: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      //分区内计算，==>("a",(2,3))===>2:次数，3：求的和
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      //分区间计算
      (t1, t2) => {
        ((t1._1 + t2._1), (t2._2 + t1._2))
      }
    )
    aggRdd.map(tup => {
      (tup._1,  tup._2._1/ tup._2._2)
    }).collect().foreach(println)


    //todo 关闭RDD
    sc.stop()
  }

}
