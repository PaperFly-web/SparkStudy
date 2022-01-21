package com.paperfly.bigdata.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_Join {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

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

        val kafkaDataDS2: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("paperfly"), kafkaPara)
        )
        val wordToOneBigData: DStream[(String, String)] = kafkaDataDS.map(v => {
            (v.value(), "bigData")
        })
//        wordToOneBigData.print()
        val wordToOnePaperFly: DStream[(String, String)] = kafkaDataDS2.map(v => {
            (v.value(), "paperfly")
        })
//        wordToOnePaperFly.print()
        val res: DStream[(String, (String, String))] = wordToOneBigData.join(wordToOnePaperFly)
        res.print()
        ssc.start()
        ssc.awaitTermination()
    }

}
