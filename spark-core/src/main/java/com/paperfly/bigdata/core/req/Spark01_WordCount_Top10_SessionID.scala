package com.paperfly.bigdata.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount_Top10_SessionID {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // 1. 读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
    //actionRDD用的比较多，那就直接缓存
    actionRDD.cache()

    //获取top10的品类ID
    val top10Ids: Array[String] = getTOP10Cid(actionRDD)
    //过滤出，TOP10的品类数据
    val filterActionRDD: RDD[String] = actionRDD.filter(line => {
      val datas: Array[String] = line.split("_")
      if (datas(6) != "-1") {
        top10Ids.contains(datas(6))
      } else {
        false
      }
    })

    val groupRdd: RDD[(String, Iterable[(String, Int)])] = filterActionRDD.map(data => {
      // 2. 根据品类ID和sessionid进行点击量的统计
      val words: Array[String] = data.split("_")
      ((words(6), words(2)), 1)
    }).reduceByKey(_ + _)
      // 3. 将统计的结果进行结构的转换
      //  (（ 品类ID，sessionId ）,sum) => ( 品类ID，（sessionId, sum） )
      .map(data => {
        (data._1._1, (data._1._2, data._2))
      })
      // 4. 相同的品类进行分组
      .groupByKey()

    // 5. 将分组后的数据进行点击量的排序，取前10名
    val result: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(value => {
      value.toList.sortBy(v => v._2)(Ordering.Int.reverse).take(10)
    })

    result.foreach(println)

    //TODO　关闭 Spark 连接
    sc.stop()
  }

  def getTOP10Cid(actionRDD: RDD[String]): Array[String] = {
    val flatMap: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(line => {
      val words: Array[String] = line.split("_")
      if (words(6) != "-1") {
        //点击的场合
        List((words(6), (1, 0, 0)))
      } else if (words(8) != "null") {
        //订单的场合
        val order: Array[String] = words(8).split(",")
        order.map(cid => (cid, (0, 1, 0)))
      } else if (words(10) != "null") {
        //支付的场合
        val pay: Array[String] = words(10).split(",")
        pay.map(cid => (cid, (0, 0, 1)))
      } else {
        Nil
      }
    })

    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatMap.reduceByKey(
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    )

    //tuple是可以进行排序的
    val result: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
    val top10Cid: Array[String] = result.map(data => {
      data._1
    })
    top10Cid
  }

}
