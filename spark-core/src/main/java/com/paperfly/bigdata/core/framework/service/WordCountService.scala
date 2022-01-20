package com.paperfly.bigdata.core.framework.service

import com.paperfly.bigdata.core.framework.common.TService
import com.paperfly.bigdata.core.framework.utils.EnvUtil
import com.paperfly.bigdata.core.req.Spark01_WordCount_Top10_SessionID.getTOP10Cid
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountService extends TService{

  override def dataAnalysis(): Any = {
    val sc: SparkContext = EnvUtil.take()
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
    result
  }
}
