package com.paperfly.bigdata.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.Random

/*
生成模拟数据，发送到kafka
 */
object SparkStreaming10_MockData {

  def main(args: Array[String]): Unit = {

    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux1:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](prop)
    while (true){
      mockData().foreach(
        data=>{
          val producerRecord = new ProducerRecord[String, String]("bigdata", data)
          producer.send(producerRecord)
          println(data)
        }
      )

      Thread.sleep(2000)
    }

  }

  def mockData() = {
    // 生成模拟数据
    // 格式 ：timestamp area city userid adid
    // 含义： 时间戳   区域  城市 用户 广告

    // Application => Kafka => SparkStreaming => Analysis
    val list = ListBuffer[String]()
    val areaList = ListBuffer[String]("华北", "华东", "华南")
    val cityList = ListBuffer[String]("北京", "上海", "深圳")

    for (i <- 1 to 30) {
      val area: String = areaList(new Random().nextInt(3))
      val city: String = cityList(new Random().nextInt(3))
      val userid: Int = new Random().nextInt(6)+1
      val adid: Int = new Random().nextInt(6)+1
      list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
    }
    list
  }

}
