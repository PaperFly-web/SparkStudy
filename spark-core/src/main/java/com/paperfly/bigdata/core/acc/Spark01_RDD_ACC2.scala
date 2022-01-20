package com.paperfly.bigdata.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//读取内存中的数据，创建RDD
object Spark01_RDD_ACC2 {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)


    //TODO 创建RDD
    val rdd: RDD[String] = sc.makeRDD(
      List("hello", "spark", "scala")
    )

    //创建累加器，并注入sc中
    val myAccMap = new MyAccMap()
    sc.register(myAccMap, "MyAccMap")

    rdd.foreach(
      word => {
        myAccMap.add(word)
      }
    )
    println(myAccMap.value)


    //todo 关闭RDD
    sc.stop()
  }

}

//定义自定义累加器
class MyAccMap extends AccumulatorV2[String, mutable.Map[String, Long]] {
  private var accMap = mutable.Map[String, Long]()

  //什么状态是作为初始化。以accMap为空的时候是初始化状态
  override def isZero: Boolean = {
    accMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
    new MyAccMap
  }

  //清空累加器。本案例就是清空accMap
  override def reset(): Unit = {
    accMap.clear()
  }

  //累加时候，添加元素。本案例，就是添加到accMap中
  override def add(word: String): Unit = {
    var newCount = accMap.getOrElse(word, 0L) + 1
    accMap.update(word, newCount)
  }

  //多个分区分别计算完后，合并他们计算的结果.Driver合并多个累加器.
  //指定other累加器的数据类型
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    val map1 = this.accMap
    val map2 = other.value

    map2.foreach {
      case (word, count) => {
        var newCount = map1.getOrElse(word, 0L) + count
        map1.update(word, newCount)
      }
    }
  }

  //获取累加器的结果。本案例，就是把accMap中的数据给他
  override def value: mutable.Map[String, Long] = {
    accMap
  }
}
