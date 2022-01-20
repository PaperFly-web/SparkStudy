package com.paperfly.bigdata.core.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

//读取内存中的数据，创建RDD
object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    // 设置检查点路径
    sc.setCheckpointDir("./checkpoint1")
    // 创建一个 RDD，读取指定位置文件:hello atguigu atguigu
    val lineRdd: RDD[String] = sc.textFile("data/1.txt")
    // 业务逻辑
    val wordRdd: RDD[String] = lineRdd.flatMap(line => line.split(" "))
    val wordToOneRdd: RDD[(String, Long)] = wordRdd.map {
      word => {
        (word, System.currentTimeMillis())
      }
    }
    // 增加缓存,避免再重新跑一个 job 做 checkpoint
    wordToOneRdd.cache()
    //缓存到文件系统中
    //wordToOneRdd.persist(StorageLevel.DISK_ONLY)

    // 数据检查点：针对 wordToOneRdd 做检查点计算
    wordToOneRdd.checkpoint()
    // 触发执行逻辑
    wordToOneRdd.collect().foreach(println)


    //todo 关闭RDD
    sc.stop()
  }

}
