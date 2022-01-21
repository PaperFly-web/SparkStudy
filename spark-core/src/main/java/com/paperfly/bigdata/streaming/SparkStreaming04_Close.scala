package com.paperfly.bigdata.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming04_Close {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "linux1:9092,linux2:9092,linux3:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "paperfly",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("bigdata"), kafkaPara)
    )
    val wordToOne: DStream[(String, Int)] = kafkaDataDS.map(v => {
      (v.value(), 1)
    })



    // TODO 窗口的范围应该是采集周期的整数倍
    // 窗口可以滑动的，但是默认情况下，一个采集周期进行滑动
    // 这样的话，可能会出现重复数据的计算，为了避免这种情况，可以改变滑动的滑动（步长）
    //第一个参数，对滑动窗口外的数据怎么处理
    //第二个参数，对刚滑动进来的数据怎么处理
    //第3个参数：采集多少秒内的数据
    //第4个参数：窗口一次滑动多少秒范围（也就是步长）
    val winDS: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow(

      (nowVal: Int, newVal: Int) => {
        //nowVal:现在窗口中的数据
        //newVal:刚滑动进来的数据
        nowVal + newVal
      },
      (nowVal: Int, oldVal: Int) => {
        //nowVal:现在窗口中的数据
        //oldVal:刚滑动过去的数据
        nowVal - oldVal
      },
      Seconds(9), Seconds(3))

    winDS.print()


    ssc.start()

    new Thread(
      new Runnable {
        override def run(): Unit = {

          Thread.sleep(500)
          val state: StreamingContextState = ssc.getState()
          if ( state == StreamingContextState.ACTIVE ) {
            ssc.stop(true, true)
          }
          System.exit(0)
        }
      }
    ).start()

    ssc.awaitTermination()
  }

}
