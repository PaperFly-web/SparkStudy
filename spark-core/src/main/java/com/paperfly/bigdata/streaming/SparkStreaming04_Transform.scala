package com.paperfly.bigdata.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_Transform {

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


        //1.其实咋一看，和map算子没什么差别。差别就在于可以执行代码的地方有不同
        wordToOne.transform(rdd=>{
            //可以在这里写代码，来一个rdd就会执行一次。在Driver端执行
            rdd.map(word=>{
                //可以在这里写代码。在Executor端执行
                word
            })
        })

        wordToOne.map(word=>{
            //可以在这里写代码。在Executor端执行
            word
        })


        ssc.start()
        ssc.awaitTermination()
    }

}
