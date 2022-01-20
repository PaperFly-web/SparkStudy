package com.paperfly.bigdata.core.framework.controller

import com.paperfly.bigdata.core.framework.common.TController
import com.paperfly.bigdata.core.framework.service.WordCountService
import org.apache.spark.rdd.RDD

class WordCountController extends TController{

  private val wordCountService = new WordCountService()

  override def dispatch(): Unit = {

    val result: Any = wordCountService.dataAnalysis()

    var resultRDD: RDD[(String, Iterable[(String, Int)])] = result.asInstanceOf[RDD[(String, Iterable[(String, Int)])]]

    resultRDD.foreach(println)
  }
}
