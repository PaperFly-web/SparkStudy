package com.paperfly.bigdata.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Random

object SparkStreaming01_DIY {

  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())
    messageDS.print()

    // 1. 启动采集器
    ssc.start()
    // 2. 等待采集器的关闭,我才主动关闭程序
    ssc.awaitTermination()
  }

  //String：采集的数据类型。
  //StorageLevel.MEMORY_ONLY：采集到的数据存储在哪里（这里是只存储在内存中）
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    var  flg = true
    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while ( flg ) {
            val message = "采集的数据为：" + new Random().nextInt(10).toString
            //采集到的数据，存储进去，等待消费
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()
    }

    override def onStop(): Unit = {
      flg = false
    }
  }
}
