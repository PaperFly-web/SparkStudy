package com.paperfly.bigdata.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount_Top10 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // 1. 读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")

    // 2. 统计品类的点击数量：（品类ID，点击数量）
    val clickRdd: RDD[(String, Int)] = actionRDD.filter(line => {
      val words: Array[String] = line.split("_")
      words(6) != "-1"
    }).map(line => {
      val words: Array[String] = line.split("_")
      (words(6), 1)
    }).reduceByKey(_ + _)

    // 3. 统计品类的下单数量：（品类ID，下单数量）
    val orderRdd: RDD[(String, Int)] = actionRDD.filter(line => {
      val words: Array[String] = line.split("_")
      words(8) != "null"
    }).flatMap(line => {
      val words: Array[String] = line.split("_")
      val order: Array[String] = words(8).split(",")
      order.map(category => {
        (category, 1)
      })
    }).reduceByKey(_ + _)

    // 4. 统计品类的支付数量：（品类ID，支付数量）
    val payRdd: RDD[(String, Int)] = actionRDD.filter(line => {
      val words: Array[String] = line.split("_")
      words(10) != "null"
    }).flatMap(line => {
      val words: Array[String] = line.split("_")
      val pay: Array[String] = words(10).split(",")
      pay.map(category => {
        (category, 1)
      })
    }).reduceByKey(_ + _)

    // 5. 将品类进行排序，并且取前10名
    //    点击数量排序，下单数量排序，支付数量排序
    //    元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
    //    ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )
    //
    //  cogroup = connect + group

    val cogroupRdd: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickRdd.cogroup(orderRdd, payRdd)

    val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRdd.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCnt = 0
        var orderCnt = 0
        var payCnt = 0

        val cliItor: Iterator[Int] = clickIter.iterator
        val ordItor: Iterator[Int] = orderIter.iterator
        val payItor: Iterator[Int] = payIter.iterator

        if (cliItor.hasNext) {
          clickCnt = cliItor.next()
        }
        if (payItor.hasNext) {
          orderCnt = payItor.next()
        }
        if (ordItor.hasNext) {
          payCnt = ordItor.next()
        }
        (clickCnt, orderCnt, payCnt)
      }
    }

    //tuple是可以进行排序的
    val result: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)

    result.foreach(println)
    //TODO　关闭 Spark 连接
    sc.stop()
  }

}
