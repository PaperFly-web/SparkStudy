package com.paperfly.bigdata.core.framework.common

import com.paperfly.bigdata.core.framework.utils.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}


trait TApplication {

  def start(master: String = "local[*]", app: String = "Application")(op: => Unit): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparkConf)

    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex => {
        println(ex.getMessage)
      }
    }

    sc.stop()
    EnvUtil.clear()


  }

}
